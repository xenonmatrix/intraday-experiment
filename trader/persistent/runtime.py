"""Persistent supervisor/worker using the existing supported Robinhood MCP transport."""
import asyncio,json,os,signal,subprocess,sys,time
from datetime import datetime,timedelta
from pathlib import Path
from ..broker import connect,BrokerToolError,failure_details
from ..models import UTC,NY,iso,dt,Unsafe
from ..calendar import Calendar
from ..service import heartbeat,pause_monitor,reconnect_delay
from ..store import atomic_text,process_lock
from ..host_health import problems as host_problems
from .db import Ledger
from .engine import Coordinator
from .risk import CFG
from .recovery import load_policy
from ..configuration import applied,apply,market_config,assert_current,fingerprint,path as config_path,warnings


def report(root,ledger,engine=None):
    r=Path(root)/'runtime';config=applied(ledger);cfg=market_config(config);now=datetime.now(UTC) if engine is None else engine.now();calendar=Calendar(config) if engine is None else engine.calendar
    session=calendar.session(now);day=now.astimezone(NY).date().isoformat();settings=ledger.settings()
    daily=ledger.session(day) or dict(loss_budget=str(settings.daily_loss),gross_losses='0',net='0',attempts=0,completed=0,opening_healthy=False,recovery_active=True)
    health=ledger.get('health',{});obs=ledger.get('observation',{});positions=ledger.trades();orders=ledger.orders()
    from ..models import D,valid_quote
    try:host=json.loads((r/'host-health.json').read_text());host_errors=host_problems(host,now,config if ledger.get('production_config') else None)
    except (OSError,ValueError):host={};host_errors=['Host health unavailable']
    try:h=json.loads((r/'heartbeat.json').read_text());heartbeat_age=now.timestamp()-h['at']
    except (OSError,ValueError,KeyError):h={};heartbeat_age=None
    blockers=[]
    if ledger.get('production_config'):
        try:assert_current(root,ledger)
        except Unsafe as exc:blockers.append(str(exc))
    if (r/'TRANSITION').exists():blockers.append('Production transition in progress')
    if not settings.live_enabled:blockers.append('Persistent LIVE disabled')
    for marker in ('READ_ONLY','KILL','MAINTENANCE','STOP','ENTRIES_DISABLED','LIQUIDATE'):
        if (r/marker).exists():blockers.append(marker)
    if not session or not session.contains(now):blockers.append('Market closed')
    elif not session.entry_window(now):blockers.append('Outside entry window')
    blockers.extend(host_errors)
    if heartbeat_age is None or not 0<=heartbeat_age<=config['watchdog']['heartbeat_stale_seconds']:blockers.append('Heartbeat not fresh for entries')
    if not health.get('reconciled') or not health.get('at') or not 0<=(now-dt(health['at'])).total_seconds()<=config['connectivity']['account_max_age_seconds']:blockers.append('Reconciliation/authentication stale or unavailable')
    if not health.get('data_healthy'):blockers.append('Market data not entry-ready')
    if not daily.get('opening_healthy'):blockers.append('Opening health pending/failed')
    if daily.get('recovery_active') or daily.get('connectivity_halt'):blockers.append('Connectivity recovery/hold')
    if daily.get('loss_latched'):blockers.append('Daily loss latch')
    if daily.get('entry_cutoff_latched') or daily.get('closed'):blockers.append('Session entry cutoff latched')
    if daily['attempts']>=settings.max_entry_attempts_per_day:blockers.append('Daily entry attempt limit')
    if any(o['status'] in ('AMBIGUOUS','DISPATCHING') for o in orders):blockers.append('Unresolved submission')
    current,reserved,allocated_risk=ledger.exposure(obs.get('quotes'))
    if len(positions)>=settings.max_open_positions:blockers.append('Position slots occupied')
    if current+reserved>=D(settings.max_total_exposure):blockers.append('Exposure capacity exhausted')
    marks_valid=True
    for t in positions:
        t['unrealized_pnl']=None;t['mark_at']=obs.get('at')
        if D(t['quantity']):
            try:
                bid,_=valid_quote(obs['quotes'][t['symbol']],now,cfg)
                t['unrealized_pnl']=str(D(t['quantity'])*bid-D(t['cost'])-D(t['entry_fees']))
            except (Unsafe,KeyError,ValueError):marks_valid=False
        t['orders']=[dict(id=o['id'],broker_id=o['broker_id'],side=o['side'],status=o['status']) for o in orders if o['trade_id']==t['id']]
    clean_orders=[dict(id=o['id'],trade_id=o['trade_id'],side=o['side'],status=o['status'],broker_id=o['broker_id'],symbol=o['data']['symbol']) for o in orders]
    state=dict(at=iso(now),architecture='persistent-v2',persistent_live_enabled=settings.live_enabled,trading_eligible_now=not blockers,
               eligibility_scope='Infrastructure/risk capacity only; every candidate still requires ORB, analysis, reservation and final broker review',
               blockers=blockers,config=settings.display(),current_exposure=str(current),reserved_exposure=str(reserved),
               total_exposure=str(current+reserved),exposure_mark_fresh=marks_valid,reserved_planned_risk=str(allocated_risk),
               open_positions=[t for t in positions if D(t['quantity'])>0],pending_positions=[t for t in positions if D(t['quantity'])==0],open_pending_position_count=len(positions),pending_unresolved_orders=clean_orders,daily_pnl=daily['net'],daily_gross_losses=daily['gross_losses'],
               daily_loss_budget=daily['loss_budget'],daily_loss_remaining=str(max(D(0),D(daily['loss_budget'])-D(daily['gross_losses']))),
               daily_unreserved_risk_remaining=str(max(D(0),D(daily['loss_budget'])-D(daily['gross_losses'])-allocated_risk)),
               trades_today=daily['completed'],entry_attempts_today=daily['attempts'],auth=health.get('auth','UNKNOWN'),
               connectivity=dict(recovering=daily.get('recovery_active',True),halt=daily.get('connectivity_halt',False),outages=daily.get('outages',0),connection=ledger.get('connection')),
               clock=host.get('clock'),host_health=dict(errors=host_errors,battery_percent=host.get('battery_percent'),min_battery_percent=config['host']['min_battery_percent'],lid_open=host.get('lid_open'),ac_power=host.get('ac_power'),sleep_prevention=host.get('sleep_prevention')),
               heartbeat=dict(age_seconds=heartbeat_age,**h),data_health=health.get('data_healthy',False),reconciliation=health,
               kill=(r/'KILL').exists(),maintenance=(r/'MAINTENANCE').exists(),hard_read_only=(r/'READ_ONLY').exists(),
               opening_health=dict(passed=daily.get('opening_healthy',False),at=daily.get('opening_checked_at'),reason=daily.get('opening_reason')),
               last_signal_rejection=ledger.get('last_signal'),entry_cutoff=iso(session.close-timedelta(minutes=session.entry_cutoff_minutes)) if session else None,
               liquidation_time=iso(session.close-timedelta(minutes=session.liquidation_minutes)) if session else None,session_date=day)
    state.update(mode='READ_ONLY' if state['hard_read_only'] else 'LIVE',config_path=str(config_path(root)),config_fingerprint=ledger.get('config_fingerprint'),strategy_fingerprint=ledger.get('strategy_fingerprint'),config_warnings=warnings(config),buying_power=obs.get('power'),entry_attempts_remaining=max(0,settings.max_entry_attempts_per_day-daily['attempts']),market_state='OPEN' if session and session.contains(now) else 'CLOSED',entry_start=iso(session.open+timedelta(minutes=session.opening_minutes)) if session else None)
    try:state['watchdog']=json.loads((r/'supervisor-health.json').read_text())
    except (OSError,ValueError):state['watchdog']={}
    if engine is not None:
        state['position_recovery_policy']=dict(mode=engine.recovery_policy.mode,
            budget_seconds=engine.recovery_policy.budget(config),fingerprint=engine.recovery_policy.fingerprint)
    atomic_text(r/'persistent-status.json',json.dumps(state,indent=2))
    return state

async def run(root,once=False):
    root=Path(root);r=root/'runtime';stop=asyncio.Event();loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):loop.add_signal_handler(sig,stop.set)
    with process_lock(r/'process.lock'):
        ledger=Ledger(r/'persistent.sqlite3')
        if (r/'PERSISTENT').exists():apply(root,ledger,datetime.now(UTC))
        from ..multistrategy.integration import make_coordinator
        engine=make_coordinator(root,ledger,recovery_policy=load_policy(root));engine.ledger.recover(engine.now());attempt=0
        config=engine.config;cc=config['connectivity'];wd=config['watchdog']
        async def pause(seconds,state):
            if wd['heartbeat_interval_seconds']==5:await pause_monitor(stop,r,seconds,state)
            else:await pause_monitor(stop,r,seconds,state,wd['heartbeat_interval_seconds'])
        heartbeat(r,'STARTING');ledger.event(engine.now(),'PERSISTENT_WORKER_START',dict(pid=os.getpid(),recovery_policy_fingerprint=engine.recovery_policy.fingerprint,recovery_mode=engine.recovery_policy.mode))
        try:
            while not stop.is_set() and not (r/'STOP').exists():
                broker=None
                try:
                    heartbeat(r,'CONNECTING')
                    async with connect(r/'oauth.json') as broker:
                        broker.bind_live_guard(engine.guard,engine.cancel_guard)
                        with ledger.tx():ledger.put('connection',broker.connection_info)
                        await engine.pin(broker);bars={};tradability={};bar_at=eligibility_at=0;good_since=time.monotonic()
                        while not stop.is_set() and not (r/'STOP').exists():
                            heartbeat(r,'POLLING');broker.cycle_error=None;broker.failure_context={};started=time.monotonic();now=engine.now();session=engine.calendar.session(now)
                            active=bool(session and session.contains(now));warm=bool(session and session.open-timedelta(minutes=config['session']['monitor_warmup_minutes'])<=now<session.open)
                            try:
                                engine.mutation_acknowledged=False
                                # Reconcile and check price/time/emergency exits before
                                # signal data; a historical endpoint outage cannot prevent
                                # known-position exits using verified current quotes.
                                if ledger.trades():await engine.manage_positions(broker,bars,tradability)
                                if active and started-eligibility_at>=config['market_data']['tradability_refresh_seconds']:
                                    tradability=await broker.tradability(engine.account,engine.cfg.universe);eligibility_at=started
                                if active and started-bar_at>=config['market_data']['bars_refresh_seconds']:
                                    bars=await broker.bars(engine.cfg.universe,session,now);bar_at=started
                                if not active:bars={};tradability={}
                                await engine.tick(broker,bars,tradability)
                                if getattr(broker,'cycle_error',None):raise broker.cycle_error
                                health=ledger.get('health',{})
                                atomic_text(r/'authentication.json',json.dumps(dict(at=health.get('at'),status=health.get('auth'),account_suffix=ledger.settings().account_suffix,connection=broker.connection_info)))
                                heartbeat(r,'ENTRIES_BLOCKED' if not ledger.settings().live_enabled else 'HEALTHY')
                                state=report(root,ledger,engine)
                                overrun=cycle_overrun(time.monotonic()-started,wd['cycle_deadline_seconds'],engine.mutation_acknowledged)
                                if overrun=='KILL':
                                    engine.failure(Unsafe('Monitor cycle exceeded configured deadline'))
                                    atomic_text(r/'KILL','Monitoring cycle deadline exceeded\n')
                                elif overrun=='SLOW_AFTER_ACK':
                                    ledger.event(engine.now(),'CYCLE_SLOW_AFTER_ACK',dict(elapsed_seconds=round(time.monotonic()-started,3)))
                                if time.monotonic()-good_since>=30:attempt=0
                                if once:return
                                await pause(max(.1,(cc['poll_seconds'] if active or warm else cc['idle_poll_seconds'])-(time.monotonic()-started)),'ENTRIES_BLOCKED' if state['blockers'] else 'HEALTHY')
                            except BrokerToolError as exc:
                                engine.failure(exc);report(root,ledger,engine);good_since=time.monotonic()
                                if once:return
                                await pause(exc.retry_after,'BACKOFF')
                except Exception as exc:
                    context=dict(getattr(exc,'recovery_context',None) or getattr(broker,'failure_context',{}) or {})
                    context['reconnect_count']=attempt
                    engine.failure(exc,context=context);report(root,ledger,engine)
                    heartbeat(r,'DISCONNECTED')
                    if once:return
                    attempt+=1;delay=min(cc['position_backoff_max_seconds'] if ledger.trades() else cc['backoff_max_seconds'],cc['backoff_initial_seconds']*2**min(attempt-1,20))
                    ledger.event(engine.now(),'RECONNECT_BACKOFF',dict(attempt=attempt,seconds=delay))
                    await pause(delay,'BACKOFF')
        finally:
            heartbeat(r,'STOPPED')
            with ledger.tx():
                ledger.put('health',dict(at=iso(engine.now()),auth='DISCONNECTED',reconciled=False,data_healthy=False))
                for t in ledger.trades():t['recovery_exit']=True;ledger.save_trade(t)
            report(root,ledger,engine);ledger.close()



def cycle_overrun(elapsed, limit, mutation_acknowledged):
    """A long cycle after an acknowledged order must not KILL or force an exit.

    KILL is for a cycle that ran long without a broker acknowledgement. The
    deadline is not evidence that an order already accepted should be flattened.
    """
    if elapsed <= limit:
        return 'OK'
    if mutation_acknowledged:
        return 'SLOW_AFTER_ACK'
    return 'KILL'


def restart_history(history, now, window):
    """Persisted rolling budget; a backward clock step cannot erase restarts."""
    import math
    if not isinstance(history,list) or any(type(x) not in (int,float) or not math.isfinite(x) for x in history):
        raise Unsafe('Invalid watchdog restart history')
    return [x for x in history if now-x<=window]


def supervise(root):
    root=Path(root);r=root/'runtime';stop=False
    def stopping(*_):
        nonlocal stop
        stop=True
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,stopping)
    with process_lock(r/'supervisor.lock'):
        atomic_text(r/'supervisor.json',json.dumps(dict(pid=os.getpid(),mode='LIVE',architecture='persistent-v2',started=time.time())))
        host=None;inhibitor=None;child=None;ledger=Ledger(r/'persistent.sqlite3')
        if sys.platform!='darwin':raise Unsafe('Production host adapter supports macOS only; checks cannot be silently disabled')
        config=apply(root,ledger,datetime.now(UTC));wd=config['watchdog'];cal=Calendar(config)
        ledger.event(datetime.now(UTC),'SUPERVISOR_START',dict(pid=os.getpid(),architecture='persistent-v2'))
        try:
            while not stop and not (r/'STOP').exists():
                if host is None or host.poll() is not None:host=subprocess.Popen([sys.executable,'-m','trader.host_health',str(r),str(os.getpid())])
                if child is None or child.poll() is not None:
                    if child is not None:
                        atomic_text(r/'KILL','Worker exited; reconciliation/recovery required\n')
                        ledger.event(datetime.now(UTC),'WORKER_EXIT',dict(pid=child.pid,exit_code=child.returncode))
                    recent=restart_history(ledger.get('worker_restarts',[]),time.time(),wd['restart_window_seconds'])
                    if len(recent)>=wd['restart_limit']:
                        atomic_text(r/'KILL','Watchdog restart budget exhausted; operator recovery required\n')
                        atomic_text(r/'supervisor-health.json',json.dumps(dict(pid=os.getpid(),at=time.time(),state='RESTART_LIMIT',sleep_prevention=bool(inhibitor and inhibitor.poll() is None))))
                        child=None;time.sleep(wd['check_interval_seconds']);continue
                    with ledger.tx():ledger.put('worker_restarts',recent+[time.time()])
                    child=subprocess.Popen([sys.executable,'-m','trader','--root',str(root),'run','--mode','LIVE'],cwd=root);born=time.time()
                now=datetime.now(UTC);session=cal.session(now)
                ledger.checkpoint(session,now)
                awake=ledger.settings().live_enabled or bool(ledger.trades()) or bool(session and session.open-timedelta(minutes=config['session']['monitor_warmup_minutes'])<=now<session.close+timedelta(minutes=5))
                if inhibitor is not None and inhibitor.poll() is not None:inhibitor=None
                if awake and inhibitor is None:inhibitor=subprocess.Popen(['/usr/bin/caffeinate','-is','-w',str(os.getpid())])
                elif not awake and inhibitor is not None:inhibitor.terminate();inhibitor.wait();inhibitor=None
                h={}
                try:
                    h=json.loads((r/'heartbeat.json').read_text());age=time.time()-h['at'] if h['pid']==child.pid else time.time()-born
                except (OSError,ValueError,KeyError):age=time.time()-born
                deadline=wd['startup_timeout_seconds'] if h.get('state') in ('STARTING','CONNECTING') else wd['restart_after_seconds']
                if age>deadline:
                    atomic_text(r/'KILL','Watchdog stalled worker; restart reconciliation required\n')
                    ledger.event(now,'WATCHDOG_STALL',dict(pid=child.pid,heartbeat_age=age,failure_class='WATCHDOG_RESTART'));child.kill();child.wait()
                atomic_text(r/'supervisor-health.json',json.dumps(dict(pid=os.getpid(),at=time.time(),state='RUNNING',worker_pid=child.pid,sleep_prevention=bool(inhibitor and inhibitor.poll() is None),config_fingerprint=fingerprint(config))))
                time.sleep(wd['check_interval_seconds'])
        finally:
            ledger.close()
            for process in (child,host,inhibitor):
                if process and process.poll() is None:
                    process.terminate()
                    try:process.wait(timeout=12)
                    except subprocess.TimeoutExpired:process.kill();process.wait()
