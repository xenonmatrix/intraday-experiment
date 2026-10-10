"""Verified, immutable broker executions; each fill belongs to exactly one trade."""
from datetime import timedelta
import json
from ..models import D, NY, Unsafe, dt, execution_identity, iso, decimal
from ..risk import TERMINAL as BROKER_TERMINAL, reconcile
from .db import TERMINAL

STATUS={'new':'SUBMITTED','queued':'SUBMITTED','confirmed':'SUBMITTED','pending_cancelled':'SUBMITTED',
        'partially_filled':'PARTIAL','filled':'FILLED','cancelled':'CANCELLED','partially_filled_rest_cancelled':'CANCELLED',
        'rejected':'REJECTED','failed':'REJECTED','voided':'REJECTED'}

def ingest(ledger,oid,actual,now):
    try:
        with ledger.tx():
            o=ledger.order(oid);p=o['data'];t=ledger.trade(o['trade_id'])
            if not o['broker_id'] or actual.get('id')!=o['broker_id']:raise Unsafe('Broker order identity mismatch')
            if any(actual.get(k)!=v for k,v in dict(symbol=t['symbol'],side=o['side'],type='market',market_hours='regular_hours').items()):raise Unsafe('Broker order shape mismatch')
            rows=actual.get('executions') or [];ids=[r['id'] for r in rows]
            total=decimal(actual['cumulative_quantity'])
            if len(ids)!=len(set(ids)) or sum((decimal(e['quantity']) for e in rows),D(0))!=total or total<D(p.get('filled_quantity','0')):raise Unsafe('Inconsistent cumulative executions')
            if o['side']=='sell' and total>D(p['quantity']):raise Unsafe('Sell overfill')
            status=STATUS.get(actual.get('state'),'AMBIGUOUS')
            if status=='FILLED' and (total<=0 or actual.get('quantity') is not None and total!=decimal(actual['quantity'])):raise Unsafe('Filled order missing complete executions')
            for e in sorted(rows,key=lambda e:(dt(e['timestamp']),e['id'])):
                q,price,fee=(decimal(e[k]) for k in ('quantity','price','fees'))
                if q<=0 or price<=0 or fee<0 or dt(e['timestamp'])>now:raise Unsafe('Invalid execution data')
                existing=ledger.db.execute('SELECT order_id,data FROM fills WHERE id=?',(e['id'],)).fetchone()
                if existing:
                    stored=json.loads(existing['data'])
                    # Match the fill's id, quantity, price and fee. A later read may
                    # add venue or timestamp fields; that must not block the stop.
                    if existing['order_id']!=oid or execution_identity(stored['execution'])!=execution_identity(e):
                        raise Unsafe('Immutable fill changed/foreign fill')
                    continue
                day=dt(e['timestamp']).astimezone(NY).date().isoformat();daily=ledger.ensure_session(day);pnl=D(0)
                if o['side']=='buy':
                    if t['status']!='OPEN':raise Unsafe('Unexpected fill for closed position')
                    if t['entered'] is None:t['entered']=e['timestamp'];t['time_exit']=iso(dt(e['timestamp'])+timedelta(minutes=t.get('max_hold_minutes',30)))
                    t['quantity']=str(D(t['quantity'])+q);t['cost']=str(D(t['cost'])+q*price);t['entry_fees']=str(D(t['entry_fees'])+fee)
                    t['entry']=str(D(t['cost'])/D(t['quantity']));t['target']=str(D(t['entry'])+D(t.get('target_r','2'))*(D(t['entry'])-D(t['stop'])))
                    p['filled_cost']=str(D(p.get('filled_cost','0'))+q*price)
                    actual_risk=D(t['quantity'])*max(D(0),D(t['entry'])-D(t['stop']))+D(t['entry_fees'])+D(t.get('config_snapshot',{}).get('execution',{}).get('fee_buffer','.01'))
                    if D(t['cost'])>D(t['notional']) or actual_risk>D(t['planned_risk']):t['recovery_exit']=True
                    t['planned_risk']=str(max(D(t['planned_risk']),actual_risk))
                else:
                    qty=D(t['quantity'])
                    if t['status']!='OPEN' or q>qty:raise Unsafe('Exit exceeds this trade ownership')
                    basis=D(t['cost'])*q/qty;entry_fee=D(t['entry_fees'])*q/qty
                    pnl=q*price-basis-entry_fee-fee
                    t['quantity']=str(qty-q);t['cost']=str(D(t['cost'])-basis);t['entry_fees']=str(D(t['entry_fees'])-entry_fee)
                    t['realized_pnl']=str(D(t['realized_pnl'])+pnl);t['exit_reason']=p['reason']
                    daily['net']=str(D(daily['net'])+pnl);daily['gross_losses']=str(D(daily['gross_losses'])+max(D(0),-pnl))
                    if D(daily['gross_losses'])>=D(daily['loss_budget']):daily['loss_latched']=True
                    if q==qty:
                        t['status']='CLOSED';t['exited']=e['timestamp'];t['planned_risk']='0';daily['completed']+=1
                    else:t['planned_risk']=str(D(t['planned_risk'])*(qty-q)/qty)
                fill=dict(execution=e,symbol=t['symbol'],side=o['side'],trade_id=t['id'],order_id=oid,
                          broker_id=o['broker_id'],config_fingerprint=t.get('config_fingerprint','legacy-unversioned'),strategy_fingerprint=t.get('strategy_fingerprint','legacy-unversioned'),realized_pnl=str(pnl),exit_reason=p.get('reason'))
                ledger.db.execute('INSERT INTO fills VALUES(?,?,?)',(e['id'],oid,json.dumps(fill)))
                ledger.save_session(day,daily);ledger.event(now,'ACTUAL_FILL',fill)
            if status in TERMINAL and o['side']=='buy' and D(t['quantity'])==0:t['status']='CLOSED';t['planned_risk']='0'
            p['filled_quantity']=str(total);p['actual']=actual
            ledger.save_trade(t);ledger.update_order(oid,status,data=p)
            ledger.event(now,'ORDER_STATUS',dict(order_id=oid,broker_id=o['broker_id'],status=status))
        if status=='AMBIGUOUS':raise Unsafe('Ambiguous broker order status')
    except BaseException:
        with ledger.tx():ledger.update_order(oid,'AMBIGUOUS')
        raise

def reconcile_owned(ledger,snapshot,account,now,cfg):
    from .recovery import RecoveryError,Failure
    try:return _reconcile_owned(ledger,snapshot,account,now,cfg)
    except (Unsafe,KeyError,ValueError,TypeError,ArithmeticError) as exc:
        raise RecoveryError(Failure.OWNERSHIP_POSITION_MISMATCH,str(exc)) from exc

def _reconcile_owned(ledger,snapshot,account,now,cfg):
    orders=ledger.orders(False);known={o['broker_id']:o for o in orders if o['broker_id']}
    pending_ids={o['broker_id'] for o in ledger.orders() if o['broker_id']}
    seen=set()
    for order in snapshot['orders']:
        oid=order.get('id')
        if oid in seen:raise Unsafe('Duplicate broker order rows')
        seen.add(oid)
        if order.get('state') not in BROKER_TERMINAL:
            local=known.get(oid)
            if not local or local['status'] in TERMINAL:raise Unsafe('Unexpected active broker order')
            t=ledger.trade(local['trade_id'])
            if order.get('symbol')!=t['symbol'] or order.get('side')!=local['side']:raise Unsafe('Working-order identity mismatch')
            if decimal(order['cumulative_quantity'])!=D(local['data'].get('filled_quantity','0')):raise Unsafe('Snapshot fills advanced; synchronize before acting')
        elif oid in pending_ids:
            raise Unsafe('Order became terminal; synchronize before acting')
    if not pending_ids<=seen:raise Unsafe('Known outstanding order missing from broker snapshot')
    expected={t['symbol']:t['quantity'] for t in ledger.trades() if D(t['quantity'])}
    filtered=dict(snapshot,orders=[])
    power=reconcile(filtered,account,now,cfg,expected)
    for p in snapshot['positions']:
        # A pending owned sell can explain a sell hold. No other hold is trusted.
        qty=D(p['quantity']);available=D(p.get('shares_available_for_sells','-1'))
        if not D(0)<=available<=qty:raise Unsafe('Unknown sellable shares')
        reserved=sum((D(o['data']['quantity'])-D(o['data'].get('filled_quantity','0')) for o in ledger.orders() if o['side']=='sell' and o['broker_id'] and ledger.trade(o['trade_id'])['symbol']==p['symbol']),D(0))
        if qty-available!=reserved:raise Unsafe('Unexplained position hold')
    return power
