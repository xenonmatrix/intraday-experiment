"""Persistent migration tests: temporary databases, deterministic clocks, fake broker only."""
import asyncio,json,uuid,threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
import pytest
from trader.models import D,Unsafe,dt,iso,down
from trader.calendar import Calendar
from trader.persistent.config import Settings
from trader.persistent.db import Ledger
from trader.persistent.engine import Coordinator
from trader.persistent.accounting import ingest,reconcile_owned
from trader.persistent.risk import CFG,proposal,check_reservation
from trader.persistent.analysis import features
from trader.persistent.cli import configure,migrate
from test_system import snapshot,quote,ACCOUNT
from test_pre_run_audit import opening

NOW=dt('2026-09-28T13:45:01Z')

def raw(now):
    return {s:[dict(b,begins_at=b['begins_at'].replace('2026-09-25',now.date().isoformat())) for b in opening()] for s in CFG.universe}
def eligible_symbols():
    return {s:dict(tradeable=True,state='active',country='US',fractional_tradability='tradable',account_type_tradabilities=[dict(account_type='individual',account_type_tradability='tradable')]) for s in CFG.universe}
def host(root,now):
    r=root/'runtime';r.mkdir(exist_ok=True)
    (r/'supervisor.json').write_text(json.dumps(dict(pid=1)))
    (r/'heartbeat.json').write_text(json.dumps(dict(pid=1,at=now.timestamp(),state='POLLING')))
    (r/'host-health.json').write_text(json.dumps(dict(at=iso(now),supervisor_pid=1,ac_power=False,battery_percent=80,lid_open=True,
        clock=dict(at=iso(now),offset_seconds=.001,uncertainty_seconds=.002))))

class Broker:
    def __init__(self,now=NOW):
        self.time=now;self.orders={};self.calls=[];self.cancels=[];self.pending=False;self.partial=False;self.timeout=False;self.error=None
        self.prices={s:(D('100.005'),D('100.01')) for s in CFG.universe};self.guard=None;self.cancel_guard=None
    async def call(self,name,args):return {'accounts':[snapshot(self.time)['account']]}
    def positions(self):
        out=[]
        for sym in CFG.universe:
            qty=sum((sum(D(e['quantity']) for e in o['executions'])*(1 if o['side']=='buy' else -1) for o in self.orders.values() if o['symbol']==sym),D(0))
            held=sum((D(o['quantity'])-D(o['cumulative_quantity']) for o in self.orders.values() if o['symbol']==sym and o['side']=='sell' and o['state'] in ('confirmed','partially_filled')),D(0))
            if qty:out.append(dict(symbol=sym,quantity=str(qty),shares_available_for_sells=str(qty-held)))
        return out
    async def snapshot(self,*args):
        if self.error:raise self.error
        s=snapshot(self.time);s['positions']=self.positions();s['orders']=list(self.orders.values());return s
    observe=snapshot
    async def quotes(self,symbols):return {s:quote(self.time,bid=str(self.prices[s][0]),ask=str(self.prices[s][1])) for s in symbols}
    async def bars(self,*args):return raw(self.time)
    async def tradability(self,*args):return eligible_symbols()
    async def review_order(self,args):return dict(args,order_checks={})
    async def submit(self,args):
        if self.guard:self.guard(args)
        self.calls.append(args);oid=str(uuid.uuid4());sym=args['symbol'];side=args['side'];price=self.prices[sym][side=='buy']
        q=down(D(args['dollar_amount'])/price) if side=='buy' else D(args['quantity'])
        filled=D(0) if self.pending else down(q/2) if self.partial else q
        executions=[] if not filled else [dict(id=str(uuid.uuid4()),quantity=str(filled),price=str(price),fees='0',timestamp=iso(self.time))]
        self.orders[oid]=dict(id=oid,symbol=sym,side=side,type='market',market_hours='regular_hours',quantity=str(q),
            cumulative_quantity=str(filled),executions=executions,state='confirmed' if self.pending else 'partially_filled' if self.partial else 'filled')
        if self.timeout:raise TimeoutError('Unknown acknowledgement')
        return {'order':{'id':oid}}
    async def order(self,account,oid):return self.orders[oid]
    async def cancel_owned(self,args):
        if self.cancel_guard:self.cancel_guard(args)
        self.cancels.append(args);o=self.orders[args['order_id']]
        o['state']='partially_filled_rest_cancelled' if D(o['cumulative_quantity']) else 'cancelled'
        return {'accepted':True}

@pytest.fixture
def env(tmp_path):
    host(tmp_path,NOW);db=Ledger(tmp_path/'runtime/persistent.sqlite3');b=Broker();e=Coordinator(tmp_path,db,lambda:b.time)
    e.account=ACCOUNT;db.put('account_number',ACCOUNT);db.configure('live_enabled',True)
    with db.tx():
        d=db.ensure_session('2026-09-28');d.update(opening_healthy=True,recovery_active=False,successes=10);db.save_session('2026-09-28',d)
    b.guard=e.guard;b.cancel_guard=e.cancel_guard
    yield e,b,db
    db.close()

def tick(e,b):asyncio.run(e.tick(b,raw(b.time),eligible_symbols()))
def observe(e,b):return asyncio.run(e.observe(b,raw(b.time),eligible_symbols()))
def reserve(e,b,symbol):
    obs=observe(e,b);c=obs['candidates'][symbol];daily=e.ledger.session(e.session().day)
    p=proposal(e.ledger.settings(),daily,c,obs['quotes'][symbol],D(obs['power']),b.time)
    def check(s,d,t,o):check_reservation(e.ledger,s,d,t,o,p,b.time,e.health_check)
    return e.ledger.reserve(e.session().day,symbol,'entry:'+c['setup_id'],p,b.time,check)

@pytest.mark.parametrize('capital,loss',[('10','.15'),('25','.375'),('50','.75'),('100','1.50')])
def test_percent_limits(capital,loss):
    s=Settings(capital_per_trade=capital).validate();assert s.trade_loss==D(loss);assert s.daily_loss==D('1.5')

def test_config_independent_and_durable(tmp_path,capsys):
    configure(tmp_path,'set-capital','25');configure(tmp_path,'set-max-positions','2')
    db=Ledger(tmp_path/'runtime/persistent.sqlite3');s=db.settings()
    assert s.capital_per_trade=='25' and s.max_total_exposure=='30' and s.max_open_positions==2 and not s.live_enabled
    db.close()

@pytest.mark.parametrize('key,value',[('capital_per_trade','NaN'),('max_total_exposure','-1'),('max_open_positions',0),('max_daily_loss_percent','101'),('buying_power_reserve','19'),('live_enabled','yes')])
def test_invalid_config_rejected(tmp_path,key,value):
    db=Ledger(tmp_path/'x.db')
    with pytest.raises(Unsafe):db.configure(key,value)
    assert db.settings()==Settings();db.close()

def test_two_simultaneous_signals_independent_positions(env):
    e,b,db=env;tick(e,b)
    assert len(b.calls)==2 and len(db.trades())==2
    assert {t['symbol'] for t in db.trades()}=={'SPY','QQQ'}
    assert len({t['entry_order_id'] for t in db.trades()})==2
    assert db.session('2026-09-28')['attempts']==2
    assert sum(D(t['cost']) for t in db.trades())<=20
    assert all(D(t['planned_risk'])<=D('.15') for t in db.trades())
    tick(e,b);assert len(b.calls)==2

def test_independent_stop_exit_maintenance_does_not_block_exit(env):
    e,b,db=env;tick(e,b);b.prices['SPY']=(D('98.98'),D('98.99'))
    (e.runtime/'MAINTENANCE').write_text('entries only')
    tick(e,b)
    assert len(b.calls)==3 and b.calls[-1]['symbol']=='SPY' and b.calls[-1]['side']=='sell'
    assert [t['symbol'] for t in db.trades()]==['QQQ']
    assert next(t for t in db.trades(False) if t['symbol']=='SPY')['exit_reason']=='INVALIDATION'

@pytest.mark.parametrize('reason',['TARGET','TIME','VWAP','SESSION_CLOSE','EMERGENCY'])
def test_exit_rules_independent(env,reason):
    e,b,db=env;tick(e,b)
    if reason=='TARGET':b.prices['SPY']=(D('103'),D('103.01'))
    elif reason=='TIME':b.time+=timedelta(minutes=30)
    elif reason=='SESSION_CLOSE':b.time=dt('2026-09-28T19:45:00Z')
    elif reason=='EMERGENCY':(e.runtime/'KILL').write_text('operator')
    obs=observe(e,b);t=next(t for t in db.trades() if t['symbol']=='SPY')
    if reason=='VWAP':
        c=obs['candidates']['SPY'];c['vwap']='101';e.last_bars=raw(b.time)
        # The bar must have closed after the fill. The fixture fill is one
        # second after that bar, so move the fill back to the bar open.
        t['entered']=c['last_bar']
    assert e.exit_reason(t,obs)==reason


def test_vwap_exit_ignores_bar_that_closed_before_fill(env):
    e,b,db=env;tick(e,b);obs=observe(e,b)
    t=next(t for t in db.trades() if t['symbol']=='SPY')
    obs['candidates']['SPY']['vwap']='101';e.last_bars=raw(b.time)
    assert dt(t['entered'])>=dt(obs['candidates']['SPY']['last_bar'])+timedelta(minutes=5)
    assert e.exit_reason(t,obs) is None


def test_read_only_blocks_exits_and_entries(env):
    e,b,db=env;tick(e,b);(e.runtime/'READ_ONLY').write_text('hard testing guard');b.prices['SPY']=(D('98'),D('98.01'))
    tick(e,b);assert len(b.calls)==2 and len(db.trades())==2

def test_pending_orders_consume_capital_and_symbol_slot(env):
    e,b,db=env;b.pending=True;tick(e,b)
    assert len(b.calls)==2;current,pending,risk=db.exposure();assert current==0 and 19<pending<=20 and risk<=D('.30')
    with pytest.raises(Unsafe,match='Duplicate'):reserve(e,b,'SPY')
    assert db.session('2026-09-28')['attempts']==2

def test_partial_fills_and_cancel_reconcile(env):
    e,b,db=env;b.partial=True;tick(e,b)
    assert len(db.trades())==2 and len(db.orders())==2
    current,reserved,_=db.exposure();assert current>0 and reserved>0 and current+reserved<=20
    b.time+=timedelta(seconds=31);host(e.root,b.time);b.partial=False
    tick(e,b)
    assert len(b.cancels)==2 and not db.orders()
    assert all(D(t['quantity'])>0 for t in db.trades())

def test_ambiguous_submission_no_duplicate_after_restart(env):
    e,b,db=env;b.timeout=True
    with pytest.raises(TimeoutError):tick(e,b)
    assert len(b.calls)==1 and db.orders()[0]['status']=='AMBIGUOUS'
    db.recover(b.time);b.timeout=False
    with pytest.raises(Unsafe):tick(e,b)
    assert len(b.calls)==1 and db.orders()[0]['status']=='AMBIGUOUS'

def test_restart_several_positions_exits_only(env):
    e,b,db=env;tick(e,b);db.recover(b.time);(e.runtime/'KILL').write_text('restart latch')
    e2=Coordinator(e.root,db,lambda:b.time);b.guard=e2.guard;b.cancel_guard=e2.cancel_guard
    tick(e2,b);assert not db.trades() and len(b.calls)==4
    assert all(a['side']=='sell' for a in b.calls[2:])

def test_restart_pending_orders_keeps_reservations(env):
    e,b,db=env;b.pending=True;tick(e,b);db.recover(b.time)
    assert len(db.orders())==2 and len(db.trades())==2 and db.exposure()[1]>0
    tick(e,b);assert len(b.calls)==2

def test_reserved_unsent_intent_aborted_on_restart(env):
    e,b,db=env;oid=reserve(e,b,'SPY');db.recover(b.time)
    assert db.order(oid)['status']=='ABORTED' and not db.trades() and db.exposure()[1]==0
    with pytest.raises(Unsafe):asyncio.run(e.dispatch(b,oid))
    assert not b.calls

@pytest.mark.parametrize('setting,value',[('max_open_positions',1),('max_total_exposure','10')])
def test_aggregate_cap(env,setting,value):
    e,b,db=env;db.configure(setting,value);tick(e,b)
    assert len(b.calls)==1 and len(db.trades())==1


def test_concurrent_reservations_cannot_overspend(env):
    e,b,db=env;db.configure('max_total_exposure','10');observe(e,b)
    path=e.runtime/'persistent.sqlite3';barrier=threading.Barrier(2)
    def worker(sym):
        local=Ledger(path)
        try:
            obs=local.get('observation');p=proposal(local.settings(),local.session('2026-09-28'),obs['candidates'][sym],obs['quotes'][sym],D(obs['power']),b.time)
            barrier.wait()
            def check(s,d,t,o):check_reservation(local,s,d,t,o,p,b.time,lambda now:None)
            try:return local.reserve('2026-09-28',sym,'entry:'+p['setup_id'],p,b.time,check)
            except Unsafe:return None
        finally:local.close()
    with ThreadPoolExecutor(2) as pool:results=list(pool.map(worker,CFG.universe))
    assert sum(bool(x) for x in results)==1 and db.exposure()[1]<=10


def test_entry_attempt_cap_and_aborted_attempt_counts(env):
    e,b,db=env;db.configure('max_entry_attempts_per_day',1);oid=reserve(e,b,'SPY');db.abort(oid,'review rejected',b.time)
    with pytest.raises(Unsafe,match='attempt'):reserve(e,b,'QQQ')
    assert db.session('2026-09-28')['attempts']==1


def test_unleveraged_reserve(env):
    e,b,db=env;original=b.snapshot
    async def low(*args):
        s=await original();s['portfolio']['buying_power'].update(buying_power='20',unleveraged_buying_power='20');return s
    b.snapshot=low;b.observe=low;tick(e,b);assert not b.calls


def test_daily_budget_is_not_refilled_by_profit_or_config_increase(env):
    e,b,db=env
    with db.tx():
        d=db.ensure_session('2026-09-28');d.update(gross_losses='1.50',net='10',loss_latched=True);db.save_session('2026-09-28',d)
    db.configure('max_total_exposure','100');tick(e,b);assert not b.calls
    with db.tx():
        assert db.ensure_session('2026-09-28')['loss_budget']=='1.5'
        fresh=db.ensure_session('2026-09-29')
    assert fresh['gross_losses']=='0' and fresh['loss_budget']=='5' and fresh['attempts']==0 and not fresh['opening_healthy']

@pytest.mark.parametrize('date',['2026-09-26T15:00:00Z','2026-12-25T15:00:00Z','2026-09-28T19:00:00Z'])
def test_weekend_holiday_cutoff(env,date):
    e,b,db=env;b.time=dt(date);host(e.root,b.time)
    with pytest.raises(Unsafe):e.health_check(b.time)

def test_early_close_calendar():
    s=Calendar().session(dt('2026-11-27T16:00:00Z'))
    assert s.close==dt('2026-11-27T18:00:00Z')
    assert s.entry_window(dt('2026-11-27T16:59:59Z')) and not s.entry_window(dt('2026-11-27T17:00:00Z'))


def test_outages_count_episodes_and_recover_without_erasing_history(env):
    e,b,db=env
    for _ in range(10):e.failure(ConnectionError('offline'))
    assert db.session('2026-09-28')['outages']==1
    b.time+=timedelta(seconds=61);e.failure(ConnectionError('offline'))
    assert db.session('2026-09-28')['connectivity_halt']
    for _ in range(31):
        b.time+=timedelta(seconds=10);host(e.root,b.time)
        e.success(b.time,raw(b.time),asyncio.run(b.quotes(CFG.universe)),True)
    d=db.session('2026-09-28');assert not d['connectivity_halt'] and not d['recovery_active'] and d['outages']==1


def test_opening_health_waits_for_real_range_and_host(env):
    e,b,db=env
    with db.tx():d=db.session('2026-09-28');d['opening_healthy']=False;db.save_session('2026-09-28',d)
    b.time=dt('2026-09-28T13:40:00Z');host(e.root,b.time);e.success(b.time,raw(b.time),asyncio.run(b.quotes(CFG.universe)),True)
    assert not db.session('2026-09-28')['opening_healthy']
    b.time=NOW;host(e.root,b.time);e.success(b.time,raw(b.time),asyncio.run(b.quotes(CFG.universe)),True)
    assert db.session('2026-09-28')['opening_healthy']


def test_feature_logging_is_causal_and_missing_not_invented(env):
    e,b,db=env;obs=observe(e,b);c=obs['candidates']['SPY'];r=raw(b.time)
    baseline=features('SPY',c,r,obs['quotes'],b.time,e.session())
    r['SPY'].append(dict(r['SPY'][-1],begins_at=iso(b.time),close_price='9999',volume=999999999))
    assert features('SPY',c,r,obs['quotes'],b.time,e.session())==baseline
    tick(e,b)
    events=[json.loads(r[0]) for r in db.db.execute("SELECT data FROM events WHERE kind='CANDIDATE_ANALYSIS'")]
    assert any(x['risk_engine_result']=='SUBMITTED_VERIFIED' for x in events)
    tick(e,b)
    assert any(json.loads(r[0])['final_reason'].startswith('Duplicate') for r in db.db.execute("SELECT data FROM events WHERE kind='CANDIDATE_ANALYSIS'"))

@pytest.mark.parametrize('fault',['stale','future','clock','battery','unknown_position','auth'])
def test_fail_closed_current_checks(env,fault):
    e,b,db=env
    if fault in ('stale','future'):
        original=b.quotes
        async def bad(symbols):
            q=await original(symbols)
            for v in q.values():v['venue_bid_time']=iso(b.time+timedelta(seconds=1 if fault=='future' else -6))
            return q
        b.quotes=bad
    elif fault in ('clock','battery'):
        p=e.runtime/'host-health.json';r=json.loads(p.read_text())
        if fault=='clock':r['clock']['offset_seconds']=.2
        else:r['battery_percent']=49
        p.write_text(json.dumps(r))
    elif fault=='unknown_position':
        original=b.snapshot
        async def bad(*args):
            s=await original();s['positions']=[dict(symbol='AAPL',quantity='1',shares_available_for_sells='1')];return s
        b.snapshot=bad;b.observe=bad
    else:b.error=PermissionError('authentication expired')
    try:tick(e,b)
    except (Unsafe,PermissionError):pass
    assert not b.calls


def test_migration_disabled_no_legacy_authorization_import(tmp_path):
    (tmp_path/'runtime').mkdir();(tmp_path/'runtime/live-approval.json').write_text('{"approved":true}')
    migrate(tmp_path);db=Ledger(tmp_path/'runtime/persistent.sqlite3')
    assert not db.settings().live_enabled and (tmp_path/'runtime/READ_ONLY').exists()
    assert (tmp_path/'runtime/live-approval.json').read_text()=='{"approved":true}'
    db.close()


def test_partial_exit_quantity_is_independent_and_fill_idempotent(env):
    e,b,db=env;tick(e,b);before={t['symbol']:D(t['quantity']) for t in db.trades()}
    b.partial=True;b.prices['SPY']=(D('98.98'),D('98.99'));tick(e,b)
    positions={t['symbol']:D(t['quantity']) for t in db.trades()}
    assert 0<positions['SPY']<before['SPY'] and positions['QQQ']==before['QQQ']
    o=next(o for o in db.orders() if o['side']=='sell');actual=b.orders[o['broker_id']]
    count=db.db.execute('SELECT count(*) FROM fills').fetchone()[0]
    ingest(db,o['id'],actual,b.time);assert db.db.execute('SELECT count(*) FROM fills').fetchone()[0]==count
    tick(e,b);assert len(b.calls)==3


def test_mutated_or_cross_position_execution_is_ambiguous(env):
    e,b,db=env;tick(e,b);o=db.orders(False)[0];actual=json.loads(json.dumps(b.orders[o['broker_id']]))
    actual['executions'][0]['price']='999'
    with pytest.raises(Unsafe,match='Immutable'):ingest(db,o['id'],actual,b.time)
    assert db.order(o['id'])['status']=='AMBIGUOUS'


def test_extra_execution_field_keeps_partial_order_actionable(env):
    e,b,db=env;b.partial=True;tick(e,b)
    o=next(o for o in db.orders() if o['side']=='buy')
    assert o['status']=='PARTIAL'
    actual=json.loads(json.dumps(b.orders[o['broker_id']]))
    actual['executions'][0]['venue']='XNYS'
    ingest(db,o['id'],actual,b.time)
    assert db.order(o['id'])['status']=='PARTIAL'
    b.prices['SPY']=(D('98.98'),D('98.99'))
    tick(e,b)
    assert any(c['side']=='sell' for c in b.calls)


def test_acknowledged_entry_is_not_a_deadline_kill(env):
    from trader.persistent.runtime import cycle_overrun
    e,b,db=env
    assert cycle_overrun(16,15,False)=='KILL'
    assert cycle_overrun(16,15,True)=='SLOW_AFTER_ACK'
    assert cycle_overrun(15,15,False)=='OK'
    tick(e,b)
    assert e.mutation_acknowledged and b.calls


def test_actual_losses_not_offset_by_profitable_trade(env):
    e,b,db=env;tick(e,b);b.prices['SPY']=(D('70'),D('70.01'));b.prices['QQQ']=(D('110'),D('110.01'))
    tick(e,b);d=db.session('2026-09-28')
    assert D(d['gross_losses'])>D('1.50') and d['loss_latched']
    assert D(d['net'])>-D(d['gross_losses']) and not db.trades()


def test_risk_sizing_smaller_than_notional_cap(env):
    e,b,db=env;obs=observe(e,b);c=dict(obs['candidates']['SPY'],invalidation='90')
    p=proposal(db.settings(),db.session('2026-09-28'),c,obs['quotes']['SPY'],D('100'),b.time)
    assert D(p['notional'])<10 and D(p['planned_risk'])<=D('.15')


def test_capital_change_does_not_increase_aggregate_exposure(env):
    e,b,db=env;db.configure('capital_per_trade','25');tick(e,b)
    assert len(db.trades())==2
    assert db.exposure()[0]<=30
    assert all(D(t['cost'])<=25 and D(t['planned_risk'])<=D('.375') for t in db.trades())


def test_cutoff_survives_clock_rollback(env):
    e,b,db=env;observe(e,b);session=e.session()
    db.checkpoint(session,dt('2026-09-28T19:00:00Z'))
    with pytest.raises(Unsafe,match='cutoff latched'):e.health_check(b.time)


def test_previous_session_failure_does_not_poison_future_session(env):
    e,b,db=env
    with db.tx():
        prior=db.ensure_session('2026-09-25');prior.update(opening_healthy=False,connectivity_halt=True,outages=99);db.save_session('2026-09-25',prior)
    tick(e,b);assert len(b.calls)==2
    assert db.session('2026-09-25')['outages']==99


def test_changed_review_blocks_without_network_submission(env):
    e,b,db=env
    async def bad(args):return dict(args,order_checks={'warning':'test'})
    b.review_order=bad;tick(e,b)
    assert not b.calls and not db.trades() and db.session('2026-09-28')['attempts']==2


def test_unknown_order_shape_not_sent(env):
    e,b,db=env;oid=reserve(e,b,'SPY')
    with db.tx():db.update_order(oid,'DISPATCHING')
    e.proof=dict(oid=oid,at=iso(b.time))
    for change in ({'symbol':'BTC'},{'type':'limit'},{'side':'sell'},{'market_hours':'extended_hours'},{'quantity':'-1'}):
        with pytest.raises(Unsafe):e.guard(dict(e.wire(oid),**change))
    assert not b.calls


def test_disabled_preference_blocks_future_days_too(env):
    e,b,db=env;db.configure('live_enabled',False);tick(e,b)
    assert not b.calls
    b.time+=timedelta(days=1);host(e.root,b.time);tick(e,b);assert not b.calls


def test_price_change_after_review_requires_revalidation(env):
    e,b,db=env
    async def moved(args):
        b.prices[args['symbol']]=(D('105'),D('105.01'));return dict(args,order_checks={})
    b.review_order=moved;tick(e,b);assert not b.calls and not db.trades()


def test_ambiguous_order_does_not_block_verified_other_position_exit(env):
    e,b,db=env;tick(e,b)
    spy=next(t for t in db.trades() if t['symbol']=='SPY')
    # Ambiguity with an ID can be resolved by a direct query. No replacement buy.
    with db.tx():db.update_order(spy['entry_order_id'],'AMBIGUOUS')
    b.prices['QQQ']=(D('98'),D('98.01'));tick(e,b)
    assert b.calls[-1]['side']=='sell' and b.calls[-1]['symbol']=='QQQ'
    assert len(b.calls)==3


def test_atomic_duplicate_reservation_same_symbol(env):
    e,b,db=env;observe(e,b);path=e.runtime/'persistent.sqlite3';barrier=threading.Barrier(2)
    def worker(_):
        local=Ledger(path)
        try:
            o=local.get('observation');p=proposal(local.settings(),local.session('2026-09-28'),o['candidates']['SPY'],o['quotes']['SPY'],D(o['power']),b.time)
            barrier.wait()
            def check(s,d,t,orders):check_reservation(local,s,d,t,orders,p,b.time,lambda now:None)
            try:return local.reserve('2026-09-28','SPY','entry:'+p['setup_id'],p,b.time,check)
            except Unsafe:return None
        finally:local.close()
    with ThreadPoolExecutor(2) as pool:results=list(pool.map(worker,range(2)))
    assert sum(bool(x) for x in results)==1 and db.session('2026-09-28')['attempts']==1


def test_unacknowledged_dispatch_blocks_second_entry(env):
    e,b,db=env;oid=reserve(e,b,'SPY')
    with db.tx():db.update_order(oid,'DISPATCHING')
    with pytest.raises(Unsafe,match='acknowledged'):reserve(e,b,'QQQ')


def test_entry_config_reduction_rechecks_reserved_order(env):
    e,b,db=env;oid=reserve(e,b,'SPY');db.configure('capital_per_trade','5')
    with pytest.raises(Unsafe):asyncio.run(e.dispatch(b,oid))
    assert not b.calls


def test_read_only_worker_reuses_one_connection(tmp_path,monkeypatch):
    from contextlib import asynccontextmanager
    from trader.persistent import runtime
    from datetime import datetime
    from trader.models import UTC
    host(tmp_path,NOW);(tmp_path/'runtime/READ_ONLY').write_text('no network mutations')
    b=Broker(datetime.now(UTC));connections=[];pauses=[]
    def bind(guard,cancel_guard=None):b.guard=guard;b.cancel_guard=cancel_guard
    b.bind_live_guard=bind;b.connection_info={'connection_id':'mock-persistent-connection'}
    @asynccontextmanager
    async def connection(*args):connections.append(1);yield b
    async def pause(stop,root,seconds,state):
        pauses.append(seconds)
        if len(pauses)>=3:stop.set()
    monkeypatch.setattr(runtime,'connect',connection);monkeypatch.setattr(runtime,'pause_monitor',pause)
    asyncio.run(runtime.run(tmp_path))
    assert len(connections)==1 and len(pauses)==3 and not b.calls
    status=json.loads((tmp_path/'runtime/persistent-status.json').read_text())
    assert status['architecture']=='persistent-v2' and status['hard_read_only'] and not status['persistent_live_enabled']


def test_exports_include_rejected_candidates_and_independent_fills(env):
    from trader.persistent.export import export
    e,b,db=env;tick(e,b);tick(e,b);out=Path(export(e.root))
    assert len((out/'fills.jsonl').read_text().splitlines())==2
    rows=[json.loads(x) for x in (out/'candidates.jsonl').read_text().splitlines()]
    assert any(r['data']['risk_engine_result']=='REJECTED' for r in rows)
    assert 'execution_id' in (out/'trades.csv').read_text().splitlines()[0]


def test_disabling_entry_preference_preserves_owned_exit(env):
    e,b,db=env;tick(e,b);db.configure('live_enabled',False)
    b.prices['SPY']=(D('98'),D('98.01'));tick(e,b)
    assert len(b.calls)==3 and b.calls[-1]['side']=='sell' and [t['symbol'] for t in db.trades()]==['QQQ']


def test_current_tool_error_stops_second_candidate(env):
    e,b,db=env
    async def failure(args):
        b.cycle_error=Unsafe('broker read failed');raise b.cycle_error
    b.review_order=failure
    with pytest.raises(Unsafe):tick(e,b)
    assert not b.calls and db.session('2026-09-28')['attempts']==1


def test_entry_health_check_does_not_imply_signal_or_order(env):
    from trader.persistent.runtime import report
    e,b,db=env;observe(e,b);state=report(e.root,db,e)
    assert state['config']['max_loss_per_trade_dollars']=='0.15'
    assert state['config']['max_daily_loss_dollars']=='1.5'
    assert not b.calls and not db.orders()


def test_status_distinguishes_pending_positions_and_risk_settings(env,capsys):
    from trader.persistent.runtime import report
    from trader.persistent.cli import status
    e,b,db=env;b.pending=True;tick(e,b);s=report(e.root,db,e)
    assert not s['open_positions'] and len(s['pending_positions'])==2 and s['open_pending_position_count']==2
    status(e.root);text=capsys.readouterr().out
    assert 'PERSISTENT LIVE ENABLED' in text and 'PER-TRADE LOSS LIMIT: 1.5%' in text and 'PENDING POSITIONS:' in text
