"""Persistent multi-position coordinator. No live capability is enabled by construction."""
from datetime import datetime,timedelta
import json,time
from pathlib import Path
from ..models import D,UTC,NY,Unsafe,PreSubmissionBlocked,bar_closed_after,dt,iso,valid_quote
from ..calendar import Calendar
from ..orb import evaluate
from ..host_health import require_host
from .db import Ledger,TERMINAL
from .accounting import ingest,reconcile_owned
from .risk import proposal,check_reservation
from .analysis import features
from ..diagnostics import redact
from .recovery import (RecoveryPolicy, RecoveryError, Failure, PositionState, REQUIRED_EVIDENCE,
                       classify, begin as begin_recovery, advance as advance_recovery)

class Coordinator:
    def __init__(self,root,ledger,clock=None,recovery_policy=None,monotonic_clock=None):
        self.root=Path(root);self.runtime=self.root/'runtime';self.ledger=ledger
        from ..configuration import applied,market_config
        self.config=applied(ledger);self.cfg=market_config(self.config)
        self.now=clock or (lambda:datetime.now(UTC));self.calendar=Calendar(self.config);self.proof=None
        self.account=ledger.get('account_number');self.last_bars={};self.last_quotes={}
        self.recovery_policy=recovery_policy or RecoveryPolicy()
        self.monotonic=monotonic_clock or time.monotonic
        # Set after a broker id or cancel is acknowledged. A slow cycle must not
        # KILL a mutation that already reached the broker.
        self.mutation_acknowledged=False

    def session(self):return self.calendar.session(self.now())
    def candidate_config(self,candidate):return self.cfg
    def health_check(self,now,entry=True):
        settings=self.ledger.settings();session=self.calendar.session(now)
        if entry and self.ledger.get('production_config'):
            from ..configuration import assert_current
            assert_current(self.root,self.ledger)
        if entry and not settings.live_enabled:raise Unsafe('Persistent LIVE entries disabled')
        if (self.runtime/'READ_ONLY').exists():raise Unsafe('Hard READ_ONLY: all broker mutations blocked')
        if (self.runtime/'STOP').exists():raise Unsafe('Process stop requested')
        if not session or not session.contains(now):raise Unsafe('Market closed')
        obs=self.ledger.get('observation',{})
        if not obs or not 0<=(now-dt(obs['at'])).total_seconds()<=self.config['execution']['preflight_max_age_seconds']:raise Unsafe('Reconciliation evidence stale')
        session_state=self.ledger.session(session.day)
        if session_state and session_state.get('closed'):raise Unsafe('Session close latched')
        if not entry:return
        if not session.entry_window(now):raise Unsafe('Outside strategy entry window')
        for marker in ('KILL','MAINTENANCE','ENTRIES_DISABLED','LIQUIDATE','TRANSITION'):
            if (self.runtime/marker).exists():raise Unsafe('Entry block: '+marker)
        require_host(self.runtime,now)
        h=json.loads((self.runtime/'heartbeat.json').read_text())
        if not 0<=now.timestamp()-h['at']<=self.config['watchdog']['heartbeat_stale_seconds'] or h['state'] in ('STOPPED','DISCONNECTED','BACKOFF'):raise Unsafe('Monitor heartbeat unhealthy')
        s=self.ledger.session(session.day)
        if not s or not s['opening_healthy']:raise Unsafe('Opening health not passed')
        if s.get('entry_cutoff_latched') or s.get('closed'):raise Unsafe('Session entry cutoff latched')
        if s['recovery_active'] or s['connectivity_halt']:raise Unsafe('Connectivity recovery incomplete')
        health=self.ledger.get('health',{})
        if health.get('auth')!='AUTHENTICATED_READ_VERIFIED' or not health.get('reconciled') or not health.get('data_healthy'):raise Unsafe('Authentication/reconciliation/data health failed')
        if not 0<=(now-dt(health['at'])).total_seconds()<=self.config['execution']['preflight_max_age_seconds']:raise Unsafe('Health evidence stale')

    def failure(self,exc,context=None,explicit=None):
        now=self.now();day=now.astimezone(NY).date().isoformat()
        with self.ledger.tx():
            s=self.ledger.ensure_session(day)
            failure=classify(exc,context,repeated=bool(s.get('outage_active')),explicit=explicit)
            if (s.get('outage_started') and (now-dt(s['outage_started'])).total_seconds()>=self.config['connectivity']['halt_after_seconds']
                    and failure['failure_class'] in (Failure.TRANSIENT_READ_TRANSPORT_FAILURE,Failure.REPEATED_BROKER_COMMUNICATION_FAILURE)):
                failure['cause_failure_class']=failure['failure_class']
                failure['failure_class']=Failure.PROLONGED_CONNECTIVITY_LOSS.value
            if not s.get('outage_active'):
                s['outages']+=1;s['outage_started']=iso(now)
            safe_reason=redact(exc)
            s.update(outage_active=True,recovery_active=True,successes=0,last_failure=iso(now),stable_since=iso(now),failure_reason=type(exc).__name__+': '+safe_reason)
            if s['outages']>=self.config['connectivity']['outage_threshold'] or (now-dt(s['outage_started'])).total_seconds()>=self.config['connectivity']['halt_after_seconds']:s['connectivity_halt']=True
            self.ledger.save_session(day,s)
            self.ledger.put('health',dict(at=iso(now),auth='UNAVAILABLE_OR_UNRECONCILED',reconciled=False,data_healthy=False,error=safe_reason))
            self.ledger.put('observation',{})
            self.ledger.event(now,'HEALTH_FAILURE',dict(day=day,reason=safe_reason,outage=s['outages'],diagnostic=failure))
            for t in self.ledger.trades():
                if D(t['quantity']):
                    conservative=self.recovery_policy.mode=='conservative_exit' or bool(t.get('recovery_exit'))
                    t['position_recovery']=begin_recovery(t.get('position_recovery'),failure,now,self.monotonic(),
                        self.recovery_policy.budget(self.config),conservative=conservative)
                    if conservative:t['position_recovery']['exit_reason']='RESTART_OR_CONNECTIVITY_RECOVERY'
                    if t['position_recovery']['state']==PositionState.VERIFIED_RISK_REDUCTION:t['recovery_exit']=True
                    self.ledger.save_trade(t)
                    self.ledger.event(now,'POSITION_RECOVERY_STARTED',dict(trade_id=t['id'],symbol=t['symbol'],recovery=t['position_recovery']))

    def success(self,now,bars,quotes,data_healthy):
        session=self.calendar.session(now);day=now.astimezone(NY).date().isoformat()
        with self.ledger.tx():
            s=self.ledger.ensure_session(day)
            if data_healthy:
                if s['successes']==0:s['stable_since']=iso(now)
                s['successes']+=1;s.setdefault('stable_since',iso(now))
                since=max(dt(s['stable_since']),dt(s.get('last_failure',s['stable_since'])))
                seconds=(now-since).total_seconds()
                if s['successes']>=self.config['connectivity']['ordinary_recovery_observations'] and seconds>=self.config['connectivity']['ordinary_recovery_seconds']:
                    if s.get('outage_active'):self.ledger.event(now,'CONNECTIVITY_RECOVERED',dict(day=day,outages=s['outages']))
                    s.update(recovery_active=False,outage_active=False)
                if s['connectivity_halt'] and seconds>=self.config['connectivity']['recovery_min_seconds'] and s['successes']>=self.config['connectivity']['recovery_observations']:
                    # Keep the original failures/count. A documented current-health
                    # recovery clears only this session's connectivity hold.
                    self.ledger.event(now,'CONNECTIVITY_HALT_RECOVERED',dict(day=day,original_outages=s['outages'],healthy_seconds=seconds))
                    s['connectivity_halt']=False
            else:s.update(recovery_active=True,successes=0,stable_since=iso(now))
            if session and session.entry_window(now) and data_healthy and not s['recovery_active'] and not s['connectivity_halt']:
                try:
                    require_host(self.runtime,now)
                    for sym in self.cfg.universe:
                        c=evaluate(sym,bars.get(sym,[]),session,now,self.cfg)
                        if not c.get('opening_high'):raise Unsafe('Opening range incomplete')
                        valid_quote(quotes[sym],now,self.cfg)
                    if not s['opening_healthy']:
                        s.update(opening_healthy=True,opening_checked_at=iso(now))
                        self.ledger.event(now,'OPENING_EQUIVALENT_HEALTH_PASSED',dict(day=day,at=iso(now),recovered_check=now>session.open+timedelta(minutes=16)))
                except (Unsafe,KeyError,ValueError) as exc:
                    s['opening_reason']=str(exc)
            self.ledger.save_session(day,s)

    async def pin(self,broker):
        if self.ledger.get('production_config') and not self.ledger.settings().account_suffix:raise Unsafe('Configure broker.account_suffix before broker monitoring')
        accounts=await broker.call('get_accounts',{})
        matches=[a for a in accounts['accounts'] if a and a.get('agentic_allowed') and a['account_number'].endswith(self.ledger.settings().account_suffix)]
        if len(matches)!=1:raise RecoveryError(Failure.AUTHENTICATION_FAILURE,'Pinned account is not uniquely available')
        account=matches[0]['account_number']
        if self.account and self.account!=account:raise RecoveryError(Failure.OWNERSHIP_POSITION_MISMATCH,'Account identity changed')
        self.account=account
        with self.ledger.tx():self.ledger.put('account_number',account)

    async def sync(self,broker):
        unknown=[o for o in self.ledger.orders() if o['status']=='AMBIGUOUS' and not o['broker_id']]
        if unknown:
            # A fresh complete broker read can resolve an exact persisted ref_id.
            # Absence is NOT evidence that a mutation failed: retain ambiguity.
            snapshot=await broker.snapshot(self.account)
            if snapshot['account']['account_number']!=self.account:
                raise RecoveryError(Failure.OWNERSHIP_POSITION_MISMATCH,'Account identity changed during intent resolution')
            for order in unknown:
                matches=[o for o in snapshot['orders'] if o.get('ref_id')==order['id']]
                if not matches:continue
                if len(matches)!=1:raise RecoveryError(Failure.AMBIGUOUS_MUTATING_ORDER_STATE,'Multiple broker orders have the same intent identity')
                actual=matches[0];trade=self.ledger.trade(order['trade_id'])
                if any(actual.get(k)!=v for k,v in dict(symbol=trade['symbol'],side=order['side'],type='market',market_hours='regular_hours',time_in_force='gfd').items()):
                    raise RecoveryError(Failure.OWNERSHIP_POSITION_MISMATCH,'Exact intent has conflicting broker shape')
                if order['side']=='sell':amount=actual.get('quantity');expected=order['data']['quantity']
                else:amount=(actual.get('dollar_based_amount') or {}).get('amount');expected=order['data']['notional']
                if amount is None or D(amount)!=D(expected) or not isinstance(actual.get('id'),str) or not actual['id']:
                    raise RecoveryError(Failure.OWNERSHIP_POSITION_MISMATCH,'Exact intent has conflicting broker amount or identity')
                with self.ledger.tx():
                    self.ledger.update_order(order['id'],'AMBIGUOUS',broker_id=actual['id'])
                    self.ledger.event(self.now(),'AMBIGUOUS_INTENT_IDENTIFIED',dict(order_id=order['id'],broker_id=actual['id'],basis='EXACT_REF_ID'))
        for order in self.ledger.orders():
            if order['broker_id']:
                actual=await broker.order(self.account,order['broker_id'])
                ingest(self.ledger,order['id'],actual,self.now())
            elif order['status']=='AMBIGUOUS':
                # Persist ambiguity; never infer an ID by symbol/quantity/time.
                continue

    async def observe(self,broker,bars=None,tradability=None,full=False):
        await self.sync(broker)
        snapshot=await (broker.snapshot(self.account) if full else broker.observe(self.account))
        now=self.now();power=reconcile_owned(self.ledger,snapshot,self.account,now,self.cfg)
        session=self.calendar.session(now);active=bool(session and session.contains(now))
        quotes=await broker.quotes(self.cfg.universe) if active else {}
        now=self.now();self.last_quotes=quotes
        candidates={};data_healthy=active
        for sym in self.cfg.universe:
            if active:
                try:
                    valid_quote(quotes[sym],now,self.cfg)
                    c=evaluate(sym,(bars or {}).get(sym,[]),session,now,self.cfg)
                    if now>=session.open+timedelta(minutes=15) and not c.get('opening_high'):raise Unsafe('Opening bars not available')
                    candidates[sym]=c
                except (Unsafe,KeyError,ValueError) as exc:
                    data_healthy=False;candidates[sym]=dict(symbol=sym,state='DATA_INVALID',reason=str(exc))
        obs=dict(at=iso(now),snapshot_at=snapshot['observed_at'],power=str(power),snapshot=snapshot,quotes=quotes,full_reconciliation=full,
                 candidates=candidates,tradability=tradability or {},data_healthy=data_healthy)
        with self.ledger.tx():
            self.ledger.put('observation',obs)
            for trade in self.ledger.trades():
                c=candidates.get(trade['symbol'],{})
                trade['strategy_state']={k:c.get(k) for k in ('state','vwap','last_bar','opening_high','opening_low')}
                trade['strategy_state']['at']=iso(now)
                self.ledger.save_trade(trade)
            self.ledger.event(now,'RECONCILIATION',dict(snapshot_at=snapshot['observed_at'],account_suffix=self.ledger.settings().account_suffix,portfolio=snapshot['portfolio'],positions=snapshot['positions'],orders=snapshot['orders'],quotes=quotes,data_healthy=data_healthy))
            for row in self.ledger.db.execute('SELECT id,data FROM fills').fetchall():
                fill=json.loads(row['data'])
                if 'account_value' not in fill:
                    fill.update(account_value=snapshot['portfolio']['total_value'],account_value_asof=snapshot['observed_at'])
                    self.ledger.db.execute('UPDATE fills SET data=? WHERE id=?',(json.dumps(fill),row['id']))
            self.ledger.put('health',dict(at=iso(now),auth='AUTHENTICATED_READ_VERIFIED',reconciled=True,data_healthy=data_healthy,regular_session=active))
        self.success(now,bars or {},quotes,data_healthy)
        return obs

    def wire(self,oid):
        o=self.ledger.order(oid);t=self.ledger.trade(o['trade_id']);p=o['data']
        args=dict(account_number=self.account,symbol=t['symbol'],side=o['side'],type='market',market_hours='regular_hours',time_in_force='gfd',ref_id=oid)
        args.update(dollar_amount=p['notional']) if o['side']=='buy' else args.update(quantity=p['quantity'])
        return args

    def guard(self,args):
        now=self.now();oid=args.get('ref_id');o=self.ledger.order(oid)
        if o['status']!='DISPATCHING' or args!=self.wire(oid):raise Unsafe('Order identity/shape changed')
        if not self.proof or self.proof['oid']!=oid or not 0<=(now-dt(self.proof['at'])).total_seconds()<=self.config['execution']['preflight_max_age_seconds']:raise Unsafe('Missing fresh order preflight')
        self.health_check(now,o['side']=='buy')
        t=self.ledger.trade(o['trade_id']);obs=self.ledger.get('observation')
        valid_quote(obs['quotes'][t['symbol']],now,self.cfg)
        reconcile_owned(self.ledger,obs['snapshot'],self.account,now,self.cfg)
        if o['side']=='buy':
            session=self.ledger.ensure_session(t['day'])
            check_reservation(self.ledger,self.ledger.settings(),session,self.ledger.trades(),self.ledger.orders(),o['data'],now,self.health_check,excluding=oid,candidate_config=self.candidate_config)
        else:
            if t['status']!='OPEN' or D(o['data']['quantity'])!=D(t['quantity']):raise Unsafe('Exit quantity/ownership changed')
            if any(x['trade_id']==t['id'] and x['id']!=oid for x in self.ledger.orders()):raise Unsafe('Another order owns this position')
            row=next((p for p in obs['snapshot']['positions'] if p['symbol']==t['symbol']),None)
            if not row or D(row['shares_available_for_sells'])<D(t['quantity']):raise Unsafe('Exit shares unavailable')

    async def dispatch(self,broker,oid):
        now=self.now()
        # RESERVED is never sent. Transition/last risk check serializes independent workers.
        with self.ledger.tx():
            o=self.ledger.order(oid)
            if o['status']!='RESERVED':raise Unsafe('Never resubmit an existing intent')
            self.health_check(now,o['side']=='buy')
            self.ledger.update_order(oid,'DISPATCHING');self.proof=dict(oid=oid,at=iso(now))
            self.guard(self.wire(oid))
        try:
            result=await broker.submit(self.wire(oid))
            bid=(result.get('order') or {}).get('id')
            if not isinstance(bid,str) or not bid:raise Unsafe('Acknowledgement missing broker ID')
            with self.ledger.tx():self.ledger.update_order(oid,'SUBMITTED',broker_id=bid)
            self.mutation_acknowledged=True
            actual=await broker.order(self.account,bid)
            ingest(self.ledger,oid,actual,self.now())
        except PreSubmissionBlocked:
            with self.ledger.tx():self.ledger.update_order(oid,'RESERVED')
            self.ledger.abort(oid,'Final transport gate blocked before network write',self.now());raise
        except BaseException:
            with self.ledger.tx():self.ledger.update_order(oid,'AMBIGUOUS')
            raise
        finally:self.proof=None

    def review_ok(self,review,args):
        if review.get('order_checks')!={}:raise Unsafe('Broker review warning or missing checks')
        for k in ('symbol','side','type'):
            if review.get(k)!=args[k]:raise Unsafe('Broker review mismatch')
        field='dollar_amount' if args['side']=='buy' else 'quantity'
        if D(review.get(field,'-1'))!=D(args[field]):raise Unsafe('Broker review amount mismatch')

    async def preflight(self,broker,oid,bars,tradability):
        o=self.ledger.order(oid);t=self.ledger.trade(o['trade_id']);args=self.wire(oid)
        self.health_check(self.now(),o['side']=='buy')
        review=await broker.review_order({k:v for k,v in args.items() if k!='ref_id'})
        self.ledger.event(self.now(),'ORDER_REVIEW',dict(order_id=oid,review=review));self.review_ok(review,args)
        if o['side']=='buy':
            session=self.session();bars=await broker.bars(self.cfg.universe,session,self.now())
            tradability=await broker.tradability(self.account,self.cfg.universe)
        await self.observe(broker,bars,tradability,full=True)
        await self.dispatch(broker,oid)
        # Acknowledgement/fill verification followed by independent reconciliation.
        await self.observe(broker,bars,tradability,full=True)

    def recovery_evidence(self,t,obs):
        """Build positive proof from current evidence, never from previous health."""
        now=self.now();proof=dict.fromkeys(REQUIRED_EVIDENCE,False)
        try:
            fresh=0<=(now-dt(obs['at'])).total_seconds()<=self.config['execution']['preflight_max_age_seconds']
            h=json.loads((self.runtime/'heartbeat.json').read_text())
            sup=json.loads((self.runtime/'supervisor-health.json').read_text())
            proof['worker_healthy']=0<=now.timestamp()-h['at']<=self.config['watchdog']['heartbeat_stale_seconds'] and h['state'] in ('POLLING','HEALTHY','ENTRIES_BLOCKED')
            proof['watchdog_healthy']=sup['state']=='RUNNING' and sup['worker_pid']==h['pid'] and 0<=now.timestamp()-sup['at']<=self.config['host']['health_max_age_seconds']
            require_host(self.runtime,now)
            proof['clock_healthy']=True
        except (Unsafe,OSError,ValueError,KeyError,TypeError):
            fresh=False
        try:
            if not fresh or not obs.get('full_reconciliation'):return proof
            snapshot=obs['snapshot']
            health=self.ledger.get('health',{})
            proof['connection_restored']=health.get('reconciled') is True
            proof['authenticated']=health.get('auth')=='AUTHENTICATED_READ_VERIFIED'
            proof['account_confirmed']=snapshot['account']['account_number']==self.account
            reconcile_owned(self.ledger,snapshot,self.account,now,self.cfg)
            for key in ('ownership_reconciled','quantity_reconciled','available_to_sell_reconciled','outstanding_orders_reconciled'):
                proof[key]=True
            proof['no_ambiguous_mutation']=not any(o['status'] in ('AMBIGUOUS','DISPATCHING') or o['data'].get('cancel_state')=='DISPATCHING' for o in self.ledger.orders())
            valid_quote(obs['quotes'][t['symbol']],now,self.cfg)
            proof['fresh_quote']=proof['quote_timestamp_valid']=True
            session=self.calendar.session(now)
            candidate=evaluate(t['symbol'],self.last_bars.get(t['symbol'],[]),session,now,self.cfg)
            # Require the latest *completed* interval, not merely an old OR.
            latest=session.open+timedelta(minutes=5*(int((now-session.open).total_seconds()//300)-1))
            proof['completed_bars_restored']=bool(candidate.get('last_bar')) and dt(candidate['last_bar'])==latest
            proof['derived_state_restored']=proof['completed_bars_restored'] and (not t.get('vwap_exit',True) or bool(candidate.get('vwap')))
            proof['durable_stop_known']=D(t['stop']).is_finite() and D(t['stop'])>0
            proof['durable_target_known']=D(t['target']).is_finite() and D(t['target'])>D(t['stop'])
            proof['original_hold_deadline_known']=bool(t['time_exit']) and dt(t['time_exit'])==dt(t['entered'])+timedelta(minutes=t.get('max_hold_minutes',30))
            proof['eod_deadline_known']=session is not None and t['day']==session.day
            settings=self.ledger.settings();daily=self.ledger.session(session.day)
            current,reserved,risk=self.ledger.exposure(obs['quotes'])
            proof['risk_consistent']=bool(daily) and not daily['loss_latched'] and current+reserved<=D(settings.max_total_exposure) and D(t['cost'])<=D(settings.capital_per_trade) and D(t['planned_risk'])<=settings.trade_loss and D(daily['gross_losses'])+risk<=D(daily['loss_budget'])
        except (Unsafe,KeyError,ValueError,TypeError,ArithmeticError):pass
        return proof

    def exit_reason(self,t,obs):
        # Protective conditions take precedence even inside bounded recovery.
        reason=self.protective_exit_reason(t,obs)
        recovery=t.get('position_recovery')
        if recovery:
            updated,decision=advance_recovery(recovery,self.now(),self.monotonic(),
                self.recovery_evidence(t,obs),protective_reason=reason)
            if updated!=recovery:
                with self.ledger.tx():
                    t['position_recovery']=updated
                    self.ledger.save_trade(t)
                    self.ledger.event(self.now(),'POSITION_RECOVERY_TRANSITION',dict(trade_id=t['id'],symbol=t['symbol'],recovery=updated))
            if decision:return decision
        if reason:return reason
        if t.get('recovery_exit'):return 'RESTART_OR_CONNECTIVITY_RECOVERY'
        return None

    def protective_exit_reason(self,t,obs):
        now=self.now();session=self.calendar.session(now)
        if not session or not session.contains(now):return None
        bid,ask=valid_quote(obs['quotes'][t['symbol']],now,self.cfg)
        settings=self.ledger.settings();daily=self.ledger.session(session.day)
        if (self.runtime/'KILL').exists() or (self.runtime/'LIQUIDATE').exists():return 'EMERGENCY'
        if t['day']!=session.day:return 'OVERNIGHT_EXCEPTION'
        if daily and daily['loss_latched']:return 'DAILY_LOSS'
        current,reserved,_=self.ledger.exposure(obs['quotes'])
        if current+reserved>D(settings.max_total_exposure) or D(t['cost'])>D(settings.capital_per_trade):return 'EXPOSURE_LIMIT'
        if D(t['planned_risk'])>settings.trade_loss:return 'PLANNED_RISK_LIMIT'
        if now>=session.close-timedelta(minutes=session.liquidation_minutes) or (daily and daily.get('liquidation_due')):return 'SESSION_CLOSE'
        if bid<=D(t['stop']):return 'INVALIDATION'
        if bid>=D(t['target']):return 'TARGET'
        candidate=obs['candidates'].get(t['symbol'],{})
        raw=self.last_bars.get(t['symbol'],[])
        if t.get('vwap_exit',True) and candidate.get('last_bar') and candidate.get('vwap') and bar_closed_after(candidate['last_bar'],t.get('entered')):
            last=next((b for b in raw if dt(b['begins_at'])==dt(candidate['last_bar'])),None)
            if last and D(last['close_price'])<D(candidate['vwap']):return 'VWAP'
        if now>=dt(t['time_exit']):return 'TIME'
        return None

    def cancel_guard(self,args):
        self.health_check(self.now(),False)
        matches=[o for o in self.ledger.orders() if o['broker_id']==args.get('order_id')]
        if len(matches)!=1 or args!={'account_number':self.account,'order_id':matches[0]['broker_id']}:
            raise Unsafe('Cancel is not a uniquely owned order')
        order=matches[0]
        if order['side']!='buy' or order['data'].get('cancel_state')!='DISPATCHING':raise Unsafe('Cancellation not reserved')
        if not self.proof or self.proof.get('cancel')!=order['id'] or not 0<=(self.now()-dt(self.proof['at'])).total_seconds()<=self.config['execution']['preflight_max_age_seconds']:raise Unsafe('Cancellation preflight stale')

    async def cancel_pending_entry(self,broker,order,reason):
        self.health_check(self.now(),False)
        if not order['broker_id'] or order['status']=='AMBIGUOUS':raise Unsafe('Unknown order cannot be blindly cancelled/replaced')
        if order['data'].get('cancel_state'):return
        with self.ledger.tx():
            current=self.ledger.order(order['id'])
            if current['status'] in TERMINAL or current['data'].get('cancel_state'):return
            data=current['data'];data.update(cancel_state='DISPATCHING',cancel_reason=reason)
            self.ledger.update_order(order['id'],current['status'],data=data)
            self.ledger.event(self.now(),'CANCEL_INTENT',dict(order_id=order['id'],broker_id=order['broker_id'],reason=reason))
        self.proof=dict(cancel=order['id'],at=iso(self.now()))
        args=dict(account_number=self.account,order_id=order['broker_id'])
        try:
            self.cancel_guard(args)
            await broker.cancel_owned(args)
            self.mutation_acknowledged=True
            actual=await broker.order(self.account,order['broker_id'])
            ingest(self.ledger,order['id'],actual,self.now())
        except BaseException:
            with self.ledger.tx():self.ledger.update_order(order['id'],'AMBIGUOUS')
            raise
        finally:self.proof=None

    async def manage_positions(self,broker,bars,tradability,obs=None):
        rebuilding=any(t.get('position_recovery',{}).get('state')==PositionState.REBUILDING for t in self.ledger.trades())
        if obs is None or rebuilding:obs=await self.observe(broker,bars,tradability,full=rebuilding)
        session=self.session();self.last_bars=bars
        for order in list(self.ledger.orders()):
            if order['side']!='buy' or not order['broker_id']:continue
            trade=self.ledger.trade(order['trade_id']);reason=None
            if D(trade['quantity']):
                try:reason=self.exit_reason(trade,obs)
                except (Unsafe,KeyError,ValueError):pass
            settings=self.ledger.settings()
            if D(order['data']['notional'])>D(settings.capital_per_trade) or D(order['data']['planned_risk'])>settings.trade_loss:reason=reason or 'RISK_SETTING_REDUCED'
            if (self.now()-dt(order['data']['checked_at'])).total_seconds()>self.config['execution']['pending_entry_timeout_seconds']:reason=reason or 'ENTRY_FILL_TIMEOUT'
            if session and self.now()>=session.close-timedelta(minutes=session.liquidation_minutes):reason=reason or 'SESSION_CLOSE'
            if (self.runtime/'KILL').exists() or (self.runtime/'LIQUIDATE').exists():reason=reason or 'EMERGENCY'
            if reason:
                try:
                    await self.cancel_pending_entry(broker,order,reason)
                    obs=await self.observe(broker,bars,tradability,full=True)
                except (Unsafe,KeyError,ValueError) as exc:
                    self.ledger.event(self.now(),'CANCEL_BLOCKED',dict(order_id=order['id'],reason=str(exc)))
                    if getattr(broker,'cycle_error',None):raise broker.cycle_error
        # Iterate stable identities, never a shared singleton position. An exit for
        # SPY cannot consume QQQ's quantity, stop, order ID or intent key.
        for trade in list(self.ledger.trades()):
            if D(trade['quantity'])<=0:continue
            try:
                reason=self.exit_reason(trade,obs)
                if reason:
                    self.health_check(self.now(),False)
                    oid=self.ledger.reserve_exit(trade['id'],reason,self.now())
                    try:await self.preflight(broker,oid,bars,tradability)
                    except Exception as exc:
                        if self.ledger.order(oid)['status']=='RESERVED':self.ledger.abort(oid,str(exc),self.now())
                        raise
                    obs=self.ledger.get('observation')
            except (Unsafe,KeyError,ValueError) as exc:
                self.ledger.event(self.now(),'EXIT_BLOCKED',dict(trade_id=trade['id'],reason=str(exc)))
                if getattr(broker,'cycle_error',None):raise broker.cycle_error
        return obs

    async def tick(self,broker,bars,tradability):
        self.last_bars=bars
        self.ledger.checkpoint(self.session(),self.now())
        obs=await self.observe(broker,bars,tradability)
        obs=await self.manage_positions(broker,bars,tradability,obs)
        for symbol in self.cfg.universe:
            now=self.now();session=self.calendar.session(now);c=obs['candidates'].get(symbol,dict(symbol=symbol,state='MARKET_CLOSED'))
            record=dict(symbol=symbol,signal=c,supporting_evidence=[],conflicting_evidence=[],entry=None,stop=c.get('invalidation'),
                        target=c.get('target'),planned_risk=None,reward_risk=None,capital=None,analysis_result='OBSERVATION_ONLY',risk_engine_result='REJECTED')
            oid=None
            try:
                record['features']=features(symbol,c,bars,obs['quotes'],now,session)
                if record['features'].get('vwap_relationship') is not None:
                    side='supporting_evidence' if D(record['features']['vwap_relationship'])>=0 else 'conflicting_evidence'
                    record[side].append('price_above_vwap' if side=='supporting_evidence' else 'price_below_vwap')
                if c.get('state')!='ARMED':raise Unsafe('ORB '+str(c.get('state')))
                record['analysis_result']='ORB_RULES_PASSED_FEATURES_LOGGED'
                settings=self.ledger.settings()
                with self.ledger.tx():daily=self.ledger.ensure_session(session.day)
                current,reserved,allocated_risk=self.ledger.exposure(obs['quotes'])
                cumulative_net=sum((D(json.loads(r[0])['net']) for r in self.ledger.db.execute('SELECT data FROM sessions')),D(0))
                available=max(D(0),D(settings.max_total_exposure)+min(D(0),cumulative_net)-current-reserved)
                available_risk=max(D(0),D(daily['loss_budget'])-D(daily['gross_losses'])-allocated_risk)
                proposed=proposal(settings,daily,c,obs['quotes'][symbol],D(obs['power'])-reserved,now,available,available_risk,cfg=self.candidate_config(c))
                record.update(entry=proposed['entry'],stop=proposed['stop'],target=proposed['target'],planned_risk=proposed['planned_risk'],reward_risk=proposed['reward_risk'],capital=proposed['notional'])
                def check(s,d,t,o):check_reservation(self.ledger,s,d,t,o,proposed,self.now(),self.health_check,candidate_config=self.candidate_config)
                oid=self.ledger.reserve(session.day,symbol,'entry:'+c['setup_id'],proposed,self.now(),check)
                record.update(risk_engine_result='RESERVED',order_intent=oid,final_reason='Passed deterministic reservation risk checks')
                self.ledger.event(now,'CANDIDATE_ANALYSIS',record)
                await self.preflight(broker,oid,bars,tradability)
                obs=self.ledger.get('observation')
                record.update(risk_engine_result='SUBMITTED_VERIFIED',final_reason='Broker order status recorded')
            except Exception as exc:
                record['final_reason']=str(exc);record['risk_engine_result']='REJECTED'
                if oid:
                    if self.ledger.order(oid)['status']=='RESERVED':self.ledger.abort(oid,str(exc),self.now())
                    record['execution_state']=self.ledger.order(oid)['status']
                    if record['execution_state']=='AMBIGUOUS':record['risk_engine_result']='SUBMISSION_AMBIGUOUS'
                if getattr(broker,'cycle_error',None):raise broker.cycle_error
                if not isinstance(exc,(Unsafe,KeyError,ValueError)):raise
            finally:
                self.ledger.event(self.now(),'CANDIDATE_ANALYSIS',record)
                with self.ledger.tx():self.ledger.put('last_signal',record)
