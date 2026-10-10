from datetime import timedelta

import httpx
import pytest

from trader.models import Unsafe, dt
from trader.persistent.recovery import (
    Failure, PositionState, RecoveryPolicy, REQUIRED_EVIDENCE,
    advance, begin, classify, entry_blocked,
)

NOW = dt('2026-10-02T14:15:27Z')


def failure(tool='get_equity_orders'):
    return classify(ExceptionGroup('transport', [httpx.RemoteProtocolError(
        'ConnectionTerminated error_code:0, last_stream_id:18621')]),
        dict(tool=tool, connection_age_seconds=10800, successful_calls=9300))


def episode():
    return begin(None, failure(), NOW, 100, 15)


def evidence():
    return dict.fromkeys(REQUIRED_EVIDENCE, True)


def test_read_goaway_preserves_metadata():
    f = failure()
    assert f['failure_class'] == Failure.TRANSIENT_READ_TRANSPORT_FAILURE
    assert f['operation'] == 'READ' and f['goaway'] and f['last_stream_id'] == 18621
    assert f['connection_age_seconds'] == 10800 and f['successful_calls'] == 9300
    assert f['exception']['children'][0]['type'] == 'RemoteProtocolError'


@pytest.mark.parametrize('tool', ['place_equity_order', 'cancel_equity_order'])
def test_mutation_is_never_certified_as_transient_read(tool):
    f = failure(tool)
    assert f['failure_class'] == Failure.AMBIGUOUS_MUTATING_ORDER_STATE
    state = begin(None, f, NOW, 100, 15)
    state, reason = advance(state, NOW + timedelta(seconds=1), 101, evidence())
    assert state['state'] == PositionState.VERIFIED_RISK_REDUCTION
    assert reason == 'RECOVERY_EXIT'


def test_transient_read_can_recover_but_entries_remain_blocked():
    state, reason = advance(episode(), NOW + timedelta(seconds=5), 105, evidence())
    assert state['state'] == PositionState.NORMAL_MANAGEMENT and reason is None
    assert entry_blocked(dict(recovery_active=True, connectivity_halt=False))


@pytest.mark.parametrize('missing', REQUIRED_EVIDENCE)
@pytest.mark.parametrize('value', [None, False, 'true', 1])
def test_each_positive_evidence_condition_is_mandatory(missing, value):
    proof = evidence(); proof[missing] = value
    state, reason = advance(episode(), NOW + timedelta(seconds=5), 105, proof)
    assert state['state'] == PositionState.REBUILDING and reason is None
    assert missing in state['missing']
    state, reason = advance(state, NOW + timedelta(seconds=15), 115, proof)
    assert reason == 'RECOVERY_DEADLINE'


def test_retries_cannot_extend_deadline():
    original = episode(); current = original
    for seconds in range(1, 15):
        current = begin(current, failure(), NOW + timedelta(seconds=seconds), 100 + seconds, 15)
    assert current['deadline'] == original['deadline']
    assert current['started_monotonic'] == original['started_monotonic']
    assert current['failures'] == 15
    assert advance(current, NOW + timedelta(seconds=15), 115, evidence())[1] == 'RECOVERY_DEADLINE'


@pytest.mark.parametrize('kind', [k for k in Failure if k != Failure.TRANSIENT_READ_TRANSPORT_FAILURE])
def test_nontransient_failures_retain_conservative_exit(kind):
    f = classify(Unsafe('injected'), explicit=kind)
    state = begin(None, f, NOW, 100, 15)
    assert advance(state, NOW, 100, evidence())[1] == 'RECOVERY_EXIT'


@pytest.mark.parametrize('reason', ['INVALIDATION', 'TARGET', 'VWAP', 'TIME', 'SESSION_CLOSE', 'EMERGENCY'])
def test_protective_exit_takes_precedence(reason):
    state, exit_reason = advance(episode(), NOW + timedelta(seconds=1), 101,
                                 evidence(), protective_reason=reason)
    assert exit_reason == reason and state['state'] == PositionState.VERIFIED_RISK_REDUCTION


@pytest.mark.parametrize('wall,mono', [(-1, 1), (5, -1), (30, 1), (1, 30), (1, float('nan'))])
def test_clock_jump_sleep_or_monotonic_reset_fails_closed(wall, mono):
    assert advance(episode(), NOW + timedelta(seconds=wall), 100 + mono, evidence())[1] == 'RECOVERY_DEADLINE'


def test_positions_are_independent():
    spy, qqq = episode(), episode()
    recovered, _ = advance(spy, NOW + timedelta(seconds=5), 105, evidence())
    waiting, _ = advance(qqq, NOW + timedelta(seconds=5), 105, {})
    assert recovered['state'] == PositionState.NORMAL_MANAGEMENT
    assert waiting['state'] == PositionState.REBUILDING
    assert qqq['state'] == PositionState.REBUILDING


def test_late_evidence_cannot_erase_exit_decision():
    state, _ = advance(episode(), NOW + timedelta(seconds=16), 116, {})
    assert advance(state, NOW + timedelta(seconds=17), 117, evidence())[1] == 'RECOVERY_DEADLINE'


def test_default_fallback_and_configured_budget():
    from trader.configuration import DEFAULTS
    policy = RecoveryPolicy()
    assert policy.mode == 'conservative_exit' and policy.budget(DEFAULTS) == 15
    assert RecoveryPolicy('bounded', 5).budget(DEFAULTS) == 5
    state = begin(None, failure(), NOW, 100, 15, conservative=True)
    assert advance(state, NOW, 100, evidence())[1] == 'RECOVERY_EXIT'


@pytest.mark.parametrize('value', [0, -1, 16, float('inf'), float('nan'), True, '10'])
def test_invalid_recovery_budget_rejected(value):
    with pytest.raises(Unsafe): RecoveryPolicy('bounded', value)


def test_auth_failure_and_no_unproven_internet_classification():
    response = httpx.Response(401, request=httpx.Request('GET', 'https://example.invalid'))
    exc = httpx.HTTPStatusError('unauthorized', request=response.request, response=response)
    assert classify(exc, dict(tool='get_accounts'))['failure_class'] == Failure.AUTHENTICATION_FAILURE
    assert classify(ConnectionError('offline'), dict(tool='get_accounts'))['failure_class'] != Failure.COMPLETE_INTERNET_LOSS


# Integration fixtures use temporary SQLite and an in-memory broker. No production
# runtime or external network is reachable from these failure-injection tests.
import asyncio
import json
from trader.models import D, iso
from test_persistent import env, tick, raw, host, eligible_symbols, observe


def prepare(env):
    e,b,db=env;tick(e,b)
    e.recovery_policy=RecoveryPolicy('bounded')
    start=b.time
    e.monotonic=lambda:100+(b.time-start).total_seconds()
    e.failure(httpx.RemoteProtocolError('GOAWAY'),context={'tool':'get_equity_orders'})
    return e,b,db


def recovered_observation(e,b,bars=None):
    host(e.root,b.time)
    (e.runtime/'supervisor-health.json').write_text(json.dumps(dict(
        at=b.time.timestamp(),worker_pid=1,state='RUNNING')))
    e.last_bars=raw(b.time) if bars is None else bars
    return asyncio.run(e.observe(b,e.last_bars,eligible_symbols(),full=True))


def test_integrated_read_recovery_does_not_exit_or_rearm(env):
    e,b,db=prepare(env);before=[(t['id'],t['stop'],t['target'],t['time_exit']) for t in db.trades()]
    b.time+=timedelta(seconds=5)
    obs=recovered_observation(e,b)
    for trade in db.trades():assert e.exit_reason(trade,obs) is None
    assert all(t['position_recovery']['state']==PositionState.NORMAL_MANAGEMENT for t in db.trades())
    assert [(t['id'],t['stop'],t['target'],t['time_exit']) for t in db.trades()]==before
    assert db.session(e.session().day)['recovery_active']
    tick(e,b)
    assert len(b.calls)==2 and db.session(e.session().day)['attempts']==2


@pytest.mark.parametrize('problem',['missing_bars','incomplete_bars','missing_vwap','clock','watchdog','worker','hold','target'])
def test_integrated_incomplete_certification_eventually_exits(env,problem):
    e,b,db=prepare(env);b.time+=timedelta(seconds=5)
    bars=raw(b.time)
    if problem=='missing_bars':bars={}
    if problem=='incomplete_bars':bars['SPY']=bars['SPY'][1:]
    obs=recovered_observation(e,b,bars)
    trade=next(t for t in db.trades() if t['symbol']=='SPY')
    if problem=='missing_vwap':
        # Positive bars alone cannot certify a required absent derived value.
        e.last_bars={}
    elif problem=='clock':
        p=e.runtime/'host-health.json';v=json.loads(p.read_text());v['clock']['offset_seconds']=1;p.write_text(json.dumps(v))
    elif problem=='watchdog':(e.runtime/'supervisor-health.json').unlink()
    elif problem=='worker':(e.runtime/'heartbeat.json').unlink()
    elif problem=='hold':trade['time_exit']=None
    elif problem=='target':trade['target']='0'
    proof=e.recovery_evidence(trade,obs)
    assert not all(proof.values())
    state,reason=advance(trade['position_recovery'],b.time+timedelta(seconds=11),116,proof)
    assert reason=='RECOVERY_DEADLINE'


@pytest.mark.parametrize('problem',['stale_quote','future_quote','missing_position','unexpected_position','quantity','sellable','ambiguous_order'])
def test_integrated_broker_uncertainty_never_certifies(env,problem):
    e,b,db=prepare(env);b.time+=timedelta(seconds=5);obs=recovered_observation(e,b)
    if problem in ('stale_quote','future_quote'):
        obs['quotes']['SPY']['venue_bid_time']=iso(b.time+timedelta(seconds=-6 if problem=='stale_quote' else 1))
    elif problem=='missing_position':obs['snapshot']['positions']=[p for p in obs['snapshot']['positions'] if p['symbol']!='SPY']
    elif problem=='unexpected_position':obs['snapshot']['positions'].append(dict(symbol='XYZ',quantity='1',shares_available_for_sells='1'))
    elif problem=='quantity':obs['snapshot']['positions'][0]['quantity']='99'
    elif problem=='sellable':obs['snapshot']['positions'][0]['shares_available_for_sells']='0'
    else:
        with db.tx():db.update_order(db.orders(False)[0]['id'],'AMBIGUOUS')
    t=next(t for t in db.trades() if t['symbol']=='SPY')
    assert not all(e.recovery_evidence(t,obs).values())
    assert len(b.calls)==2


@pytest.mark.parametrize('reason',['INVALIDATION','TARGET','TIME','SESSION_CLOSE','VWAP'])
def test_integrated_protective_exit_during_recovery(env,reason):
    e,b,db=prepare(env)
    if reason=='INVALIDATION':b.prices['SPY']=(D('98'),D('98.01'))
    if reason=='TARGET':b.prices['SPY']=(D('103'),D('103.01'))
    if reason=='TIME':b.time+=timedelta(minutes=30)
    if reason=='SESSION_CLOSE':b.time=dt('2026-09-28T19:45:00Z')
    obs=recovered_observation(e,b)
    trade=next(t for t in db.trades() if t['symbol']=='SPY')
    if reason=='VWAP':
        obs['candidates']['SPY']['vwap']='101'
        trade['entered']=obs['candidates']['SPY']['last_bar']
    assert e.exit_reason(trade,obs)==reason


def test_integrated_deadline_then_verified_exit_once(env):
    e,b,db=prepare(env);b.time+=timedelta(seconds=16);recovered_observation(e,b)
    tick(e,b);assert not db.trades() and len(b.calls)==4
    assert all(t['exit_reason']=='RECOVERY_DEADLINE' for t in db.trades(False))
    tick(e,b);assert len(b.calls)==4


@pytest.mark.parametrize('side',['buy','sell'])
def test_goaway_after_mutation_stays_ambiguous_without_duplicate(env,side):
    e,b,db=env
    if side=='sell':tick(e,b);b.prices['SPY']=(D('98'),D('98.01'))
    before=len(b.calls);original=b.submit
    async def submit(args):
        await original(args)
        raise httpx.RemoteProtocolError('GOAWAY after write')
    b.submit=submit
    with pytest.raises(httpx.RemoteProtocolError):tick(e,b)
    assert len(b.calls)==before+1 and any(o['status']=='AMBIGUOUS' for o in db.orders())
    b.submit=original
    with pytest.raises(Unsafe):tick(e,b)
    assert len(b.calls)==before+1


def test_goaway_after_cancel_does_not_repeat_cancel(env):
    e,b,db=env;b.pending=True;tick(e,b);b.time+=timedelta(seconds=31);host(e.root,b.time)
    original=b.cancel_owned
    async def cancel(args):
        await original(args)
        raise httpx.RemoteProtocolError('GOAWAY after cancel')
    b.cancel_owned=cancel
    with pytest.raises(httpx.RemoteProtocolError):tick(e,b)
    assert len(b.cancels)==1
    assert any(o['status']=='AMBIGUOUS' for o in db.orders())
    b.cancel_owned=original
    tick(e,b)
    assert len({a['order_id'] for a in b.cancels})==len(b.cancels)


def test_partial_fill_remains_owned_during_recovery(env):
    e,b,db=env;b.partial=True;tick(e,b);e.recovery_policy=RecoveryPolicy('bounded')
    e.failure(httpx.RemoteProtocolError('GOAWAY'),context={'tool':'get_equity_orders'})
    qty={t['symbol']:t['quantity'] for t in db.trades()}
    obs=recovered_observation(e,b)
    assert {t['symbol']:t['quantity'] for t in db.trades()}==qty
    assert len(db.orders())==2 and len(b.calls)==2
    assert all(e.recovery_evidence(t,obs)['quantity_reconciled'] for t in db.trades())


def test_single_symbol_incomplete_bars_does_not_corrupt_other(env):
    e,b,db=prepare(env);b.time+=timedelta(seconds=5)
    bars=raw(b.time);bars['SPY']=[];obs=recovered_observation(e,b,bars)
    for t in db.trades():assert e.exit_reason(t,obs) is None
    states={t['symbol']:t['position_recovery']['state'] for t in db.trades()}
    assert states=={'SPY':PositionState.REBUILDING,'QQQ':PositionState.NORMAL_MANAGEMENT}


@pytest.mark.parametrize('side',['buy','sell'])
def test_exact_ref_id_reconciles_unknown_ack_without_resubmitting(env,side):
    e,b,db=env
    if side=='sell':tick(e,b);b.prices['SPY']=(D('98'),D('98.01'))
    original=b.submit;before=len(b.calls)
    async def submit(args):
        result=await original(args);order=b.orders[result['order']['id']]
        order.update(ref_id=args['ref_id'],time_in_force='gfd',
                     dollar_based_amount={'amount':args['dollar_amount']} if side=='buy' else None)
        raise httpx.RemoteProtocolError('GOAWAY after broker accepted')
    b.submit=submit
    with pytest.raises(httpx.RemoteProtocolError):tick(e,b)
    b.submit=original
    asyncio.run(e.sync(b))
    assert len(b.calls)==before+1
    assert not any(o['status']=='AMBIGUOUS' for o in db.orders())
    assert len(db.db.execute('select * from fills').fetchall())==before+1
    asyncio.run(e.sync(b))
    assert len(b.calls)==before+1


def test_duplicate_ref_id_remains_ambiguous(env):
    from copy import deepcopy
    e,b,db=env;b.timeout=True
    with pytest.raises(TimeoutError):tick(e,b)
    order=next(iter(b.orders.values()));order['ref_id']=db.orders()[0]['id']
    b.orders['duplicate']=deepcopy(order)
    with pytest.raises(Unsafe,match='Multiple broker orders'):asyncio.run(e.sync(b))
    assert db.orders()[0]['status']=='AMBIGUOUS' and len(b.calls)==1


@pytest.mark.parametrize('problem',['symbol','side','amount'])
def test_conflicting_ref_id_shape_cannot_be_adopted(env,problem):
    e,b,db=env;b.timeout=True
    with pytest.raises(TimeoutError):tick(e,b)
    order=next(iter(b.orders.values()));local=db.orders()[0]
    order.update(ref_id=local['id'],time_in_force='gfd',dollar_based_amount={'amount':local['data']['notional']})
    if problem=='symbol':order['symbol']='QQQ'
    elif problem=='side':order['side']='sell'
    else:order['dollar_based_amount']['amount']='999'
    with pytest.raises(Unsafe,match='conflicting'):asyncio.run(e.sync(b))
    assert db.orders()[0]['status']=='AMBIGUOUS' and not db.orders()[0]['broker_id']


def test_recovery_stability_starts_at_first_healthy_observation(env):
    e,b,db=env;e.failure(ConnectionError('offline'))
    b.time+=timedelta(seconds=60)
    for _ in range(3):
        e.success(b.time,raw(b.time),asyncio.run(b.quotes(e.cfg.universe)),True)
        b.time+=timedelta(seconds=1)
    assert db.session(e.session().day)['recovery_active']


def test_restart_never_certifies_previous_process_episode(env):
    e,b,db=prepare(env);db.recover(b.time)
    obs=recovered_observation(e,b)
    assert all(e.exit_reason(t,obs)=='WORKER_PROCESS_FAILURE' for t in db.trades())


@pytest.mark.parametrize('text',['[]','{}','{"mode":"bounded","max_seconds":999}',
    '{"mode":"bounded","mode":"conservative_exit","max_seconds":5}',
    '{"mode":"bounded","max_seconds":NaN}',
    '{"mode":"bounded","max_seconds":5,"unknown":true}'])
def test_startup_policy_is_strict(tmp_path,text):
    from trader.persistent.recovery import load_policy
    p=tmp_path/'config/position-recovery.json';p.parent.mkdir();p.write_text(text)
    with pytest.raises(Unsafe):load_policy(tmp_path)


def test_startup_policy_defaults_and_fingerprint(tmp_path):
    from trader.persistent.recovery import load_policy
    baseline=load_policy(tmp_path)
    assert baseline.mode=='conservative_exit'
    p=tmp_path/'config/position-recovery.json';p.parent.mkdir();p.write_text('{"mode":"bounded","max_seconds":5}')
    assert load_policy(tmp_path).fingerprint!=baseline.fingerprint


def test_http_phase_context_preserves_read_identity(tmp_path):
    from trader.diagnostics import hooks
    context={};h=hooks(tmp_path,'synthetic',failure_context=context)
    request=httpx.Request('POST','https://example.invalid/mcp/trading',json={
        'method':'tools/call','id':1,'params':{'name':'get_equity_orders'}})
    async def run():
        await h['request'][0](request)
        await request.extensions['trace']('http2.receive_response_headers.failed',
            {'exception':httpx.RemoteProtocolError('GOAWAY last_stream_id:31')})
    asyncio.run(run())
    assert context['tool']=='get_equity_orders'
    assert context['http_phase']=='http2.receive_response_headers.failed'


def test_failure_event_redacts_auth_material(env):
    e,b,db=env
    secret='synthetic-'+('x'*40)
    e.failure(ConnectionError('Bearer '+secret))
    assert secret not in db.get('health')['error']
    assert secret not in db.db.execute("select data from events where kind='HEALTH_FAILURE' order by id desc limit 1").fetchone()[0]


def test_prolonged_loss_classification_and_immutable_budget(env):
    e,b,db=prepare(env);deadlines={t['id']:t['position_recovery']['deadline'] for t in db.trades()}
    b.time+=timedelta(seconds=61)
    e.failure(ConnectionError('offline'),context={'tool':'get_equity_orders'})
    for t in db.trades():
        recovery=t['position_recovery']
        assert recovery['deadline']==deadlines[t['id']]
        assert recovery['last_failure']['failure_class']==Failure.PROLONGED_CONNECTIVITY_LOSS
        assert recovery['state']==PositionState.VERIFIED_RISK_REDUCTION
