import asyncio
from datetime import datetime, timedelta
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from .broker import connect, failure_types, failure_details, BrokerToolError
from .calendar import Calendar
from .engine import Engine
from .live import LiveEngine
from .models import Mode, UTC, Unsafe, iso, valid_quote
from .store import Store, atomic_text, process_lock
from .unattended import Unattended, audit
from .probe import active as probe_active
from .diagnostics import exception_record


def logger(root):
    log = logging.getLogger("intraday")
    log.setLevel(logging.INFO)
    if not log.handlers:
        handler = RotatingFileHandler(root / "service.log", maxBytes=2_000_000, backupCount=5)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(handler)
        log.addHandler(logging.StreamHandler())
    return log


def heartbeat(root, state):
    atomic_text(root / "heartbeat.json", json.dumps({"at": time.time(), "pid": os.getpid(), "state": state}))


async def pause_monitor(stopping, runtime, seconds, state, heartbeat_interval=5):
    """Backoff stays interruptible and observable; it never implies broker health."""
    deadline=time.monotonic()+seconds
    while not stopping.is_set() and time.monotonic()<deadline:
        heartbeat(runtime,state)
        try:
            await asyncio.wait_for(stopping.wait(),timeout=min(heartbeat_interval,max(.01,deadline-time.monotonic())))
        except TimeoutError:
            pass


def reconnect_delay(attempt, has_position=False):
    return min(5 if has_position else 30,2**min(attempt,5))


async def run(root, mode=Mode.PAPER, once=False):
    if mode == Mode.LIVE and (Path(root)/'runtime/PERSISTENT').exists():
        from .persistent.runtime import run as persistent_run
        return await persistent_run(root, once)
    root=Path(root).resolve();runtime=root/'runtime';directory=runtime/mode.value
    directory.mkdir(parents=True,exist_ok=True);os.chmod(runtime,0o700)
    log=logger(runtime);stopping=asyncio.Event();loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT): loop.add_signal_handler(sig,stopping.set)
    with process_lock(runtime/'process.lock'):
        store=Store(directory/'state.sqlite3')
        engine=LiveEngine(store,directory) if mode==Mode.LIVE else Engine(store,directory,mode)
        calendar=Calendar();attempt=0
        if mode==Mode.LIVE:
            engine.unattended.health(datetime.now(UTC),False,reason='Worker starting; reconciliation required')
            audit(runtime,datetime.now(UTC),'WORKER_START',mode=mode.value)
        heartbeat(runtime,'STARTING')

        def failed(exc,broker=None):
            reason=', '.join(failure_details(exc))
            now=datetime.now(UTC)
            log.error('Entries blocked: %s',reason)
            audit(runtime,now,'MONITOR_EXCEPTION',exception=exception_record(exc),tool=getattr(broker,'last_failed_tool',None),connection=getattr(broker,'connection_info',{}))
            if mode==Mode.LIVE:
                engine.unattended.failure(store,now,reason)
            atomic_text(runtime/'authentication.json',json.dumps(dict(at=iso(now),status='UNAVAILABLE_OR_NOT_AUTHENTICATED',reason=reason,tool=getattr(broker,'last_failed_tool',None))))
            engine.block(now,reason);engine.report(now,calendar.session(now))
            heartbeat(runtime,'DISCONNECTED')
            if store.get('position'): store.save('recovery_exit',True)

        try:
            while not stopping.is_set() and not (runtime/'STOP').exists():
                active_broker=None
                try:
                    heartbeat(runtime,'CONNECTING')
                    async with connect(runtime/'oauth.json') as broker:
                        active_broker=broker
                        if mode==Mode.LIVE:
                            broker.bind_live_guard(engine.write_guard,engine.cancel_guard)
                            audit(runtime,datetime.now(UTC),'MCP_CONNECTED',**broker.connection_info)
                        if not engine.account:
                            result=await broker.call('get_accounts',{})
                            accounts=[a for a in result['accounts'] if a and a.get('agentic_allowed') and a.get('nickname')=='Agentic' and a['account_number'].endswith(__import__('trader.configuration',fromlist=['load']).load(root)['broker']['account_suffix'])]
                            if len(accounts)!=1: raise Unsafe('Configured Agentic account is not uniquely identifiable')
                            engine.account=accounts[0]['account_number'];store.save('account_number',engine.account)
                        snapshot,bars,tradability=None,{},{}
                        account_at=bars_at=eligibility_at=0
                        healthy_since=time.monotonic()
                        while not stopping.is_set() and not (runtime/'STOP').exists():
                            started=time.monotonic();heartbeat(runtime,'POLLING');broker.cycle_error=None
                            if mode==Mode.LIVE: engine.mutation_acknowledged=False
                            now=datetime.now(UTC);session=calendar.session(now)
                            active=bool(session and session.contains(now))
                            probe=probe_active(runtime,now)
                            warmup=bool(session and session.open-timedelta(minutes=10)<=now<session.open)
                            held=store.get('position');symbols=[held['symbol']] if held else engine.cfg.universe
                            priority_exit=mode==Mode.LIVE and bool(held or store.unresolved()) and (engine.unattended.liquidation_due(now) or store.get('recovery_exit') or any((runtime/f).exists() for f in ('KILL','LIQUIDATE')))
                            try:
                                # PAPER retains its separate observation cadence.
                                if mode!=Mode.LIVE and (started-account_at>=(8 if active or warmup else 60) or snapshot is None):
                                    snapshot=await broker.snapshot(engine.account);account_at=started
                                if active and not held and not priority_exit and (started-eligibility_at>=60 or not tradability):
                                    tradability=await broker.tradability(engine.account,engine.cfg.universe);eligibility_at=started
                                if active and not priority_exit and (started-bars_at>=20 or not bars):
                                    try:
                                        bars=await broker.bars(symbols,session,now);bars_at=started
                                    except Exception as exc:
                                        bars={}
                                        store.event(iso(datetime.now(UTC)),'MARKET_DATA_UNAVAILABLE',dict(reason=', '.join(failure_details(exc))))
                                        if broker.cycle_error: raise broker.cycle_error
                                # LIVE engine polls accounts/orders then fresh quotes in
                                # that order, after syncing any actual fills.
                                quotes=await broker.quotes(symbols) if active and mode!=Mode.LIVE else {}
                                if active and time.monotonic()-started>engine.cfg.heartbeat_timeout:
                                    atomic_text(runtime/'ENTRIES_DISABLED','Market/account collection missed monitoring deadline\n');store.save('monitor_deadline_failed',True)
                                await engine.tick(broker,datetime.now(UTC),session,snapshot,quotes,bars,tradability)
                                if broker.cycle_error: raise broker.cycle_error
                                if mode==Mode.LIVE:
                                    quotes=engine.last_quotes
                                observed=datetime.now(UTC)
                                reconciled=mode!=Mode.LIVE or json.loads((runtime/'readiness.json').read_text()).get('healthy') is True
                                fresh=active;quote_evidence={};quote_responses_healthy=False
                                if active:
                                    fresh=True
                                    for symbol in ([held['symbol']] if held else ['SPY','QQQ']):
                                        try:
                                            q=quotes[symbol];valid_quote(q,observed,engine.cfg)
                                            quote_evidence[symbol]={k:q[k] for k in ('bid_price','ask_price','venue_bid_time','venue_ask_time')}
                                        except (Unsafe,KeyError): fresh=False
                                    quote_responses_healthy=fresh
                                elif probe:
                                    from ops.connectivity_soak import quote_check
                                    try:
                                        quote_evidence=quote_check(quotes,observed,session,True)
                                        quote_responses_healthy=True
                                    except (Unsafe,KeyError,ValueError):
                                        quote_responses_healthy=False
                                healthy_observation=reconciled and (fresh if active else (quote_responses_healthy if probe else True))
                                if mode==Mode.LIVE and healthy_observation:
                                    engine.unattended.success(store,observed)
                                if healthy_observation and time.monotonic()-healthy_since>=30: attempt=0
                                atomic_text(runtime/'authentication.json',json.dumps(dict(at=iso(observed),status='AUTHENTICATED_READ_VERIFIED' if reconciled else 'AUTHENTICATED_BUT_RECONCILIATION_UNHEALTHY',account_suffix=engine.account[-4:],connection=broker.connection_info)))
                                elapsed=time.monotonic()-started
                                snap=store.get('broker_snapshot',{})
                                cycle=dict(at=iso(observed),pid=os.getpid(),connection_id=broker.connection_info.get('connection_id'),elapsed_seconds=round(elapsed,3),reconciled=reconciled,fresh_quotes=fresh,quotes=quote_evidence,quote_responses_healthy=quote_responses_healthy,regular_session=active,read_only_probe=probe,position=store.get('position'),unresolved_orders=len(store.unresolved()),snapshot_at=snap.get('observed_at'),maintenance=(runtime/'MAINTENANCE').exists(),call_counts=broker.call_counts)
                                atomic_text(runtime/'cycle.json',json.dumps(cycle))
                                audit(runtime,observed,'MONITOR_CYCLE',**{k:v for k,v in cycle.items() if k not in ('at','pid')})
                                heartbeat(runtime,'HEALTHY' if store.get('gate',{}).get('entries_allowed') else 'ENTRIES_BLOCKED')
                                if once: return
                                if elapsed>engine.cfg.heartbeat_timeout and not getattr(engine,'mutation_acknowledged',False):
                                    atomic_text(runtime/'KILL','Monitoring cycle exceeded deadline\n');engine.block(observed,'Monitoring latency exceeded safety deadline')
                                elif elapsed>engine.cfg.heartbeat_timeout:
                                    audit(runtime,observed,'CYCLE_SLOW_AFTER_ACK',elapsed_seconds=round(elapsed,3))
                                delay=max(.1,(engine.cfg.poll_seconds if active or warmup or probe else 30)-elapsed)
                                try: await asyncio.wait_for(stopping.wait(),timeout=delay)
                                except TimeoutError: pass
                            except BrokerToolError as exc:
                                # A returned MCP tool error does not destroy a healthy
                                # session. Back off without retrying any intended order.
                                failed(exc,broker);healthy_since=time.monotonic()
                                if once: return
                                await pause_monitor(stopping,runtime,exc.retry_after,'BACKOFF')
                except Exception as exc:
                    failed(exc,active_broker)
                    if once: return
                    attempt+=1
                    delay=reconnect_delay(attempt,bool(store.get('position')))
                    audit(runtime,datetime.now(UTC),'RECONNECT_BACKOFF',attempt=attempt,seconds=delay)
                    await pause_monitor(stopping,runtime,delay,'BACKOFF')
        finally:
            if store.get('position'):
                atomic_text(runtime/'KILL','Process stopped with open position; recovery liquidation required\n');store.save('recovery_exit',True)
            prior=store.get('gate',{}).get('reason','No prior observation')
            engine.block(datetime.now(UTC),'Process stopped; last gate: '+prior)
            engine.report(datetime.now(UTC),calendar.session(datetime.now(UTC)));heartbeat(runtime,'STOPPED')
            if mode==Mode.LIVE:
                engine.unattended.health(datetime.now(UTC),False,reason='Worker stopped')
                audit(runtime,datetime.now(UTC),'WORKER_STOP',position=store.get('position'))
            store.close()


def supervise(root, mode):
    """Independent process watchdog. Never trades; latches KILL on crash/hang."""
    if mode == Mode.LIVE and (Path(root)/"runtime/PERSISTENT").exists():
        from .persistent.runtime import supervise as persistent_supervise
        return persistent_supervise(root)
    root = Path(root).resolve()
    runtime = root / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    stop = False
    def shutdown(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    with process_lock(runtime / "supervisor.lock"):
        atomic_text(runtime/"supervisor.json",json.dumps({"pid":os.getpid(),"mode":mode.value,"started":time.time()}))
        inhibitor = None
        host_monitor = None
        sleep_policy=None
        if mode==Mode.LIVE:
            from .live_policy import LivePolicy
            sleep_policy=LivePolicy(root)
            unattended=Unattended(sleep_policy)
            audit(runtime,datetime.now(UTC),'SUPERVISOR_START')
        while not stop and not (runtime/"STOP").exists():
            if mode == Mode.LIVE and sys.platform == 'darwin' and (host_monitor is None or host_monitor.poll() is not None):
                host_monitor = subprocess.Popen([sys.executable, '-m', 'trader.host_health', str(runtime), str(os.getpid())])
            child = subprocess.Popen([sys.executable, "-m", "trader", "--root", str(root), "run", "--mode", mode.value], cwd=Path(__file__).resolve().parent.parent)
            born = time.time()
            while child.poll() is None and not stop and not (runtime/"STOP").exists():
                time.sleep(1)
                if host_monitor is not None and host_monitor.poll() is not None:
                    audit(runtime,datetime.now(UTC),'HOST_MONITOR_RESTART',exit_code=host_monitor.returncode)
                    host_monitor = subprocess.Popen([sys.executable, '-m', 'trader.host_health', str(runtime), str(os.getpid())])
                if sleep_policy is not None and sys.platform=='darwin':
                    keep_awake = unattended.keep_awake(datetime.now(UTC))
                    if inhibitor is not None and inhibitor.poll() is not None:
                        audit(runtime,datetime.now(UTC),'SLEEP_INHIBITOR_EXIT',exit_code=inhibitor.returncode)
                        inhibitor = None
                    if keep_awake and inhibitor is None:
                        inhibitor = subprocess.Popen(["/usr/bin/caffeinate","-is","-w",str(os.getpid())])
                    elif not keep_awake and inhibitor is not None:
                        inhibitor.terminate()
                        inhibitor.wait()
                        inhibitor = None
                try:
                    h = json.loads((runtime / "heartbeat.json").read_text())
                    age = time.time()-h["at"] if h["pid"] == child.pid else time.time()-born
                except (FileNotFoundError, ValueError, KeyError):
                    age = time.time()-born
                    h = {}
                if sleep_policy is not None:
                    unattended.checkpoint(datetime.now(UTC),child.pid,h)
                # Idle sessions heartbeat every <=30s. During active polls allow
                # 45s total transport budget; entry checks still require <=5s data.
                if age > 45:
                    atomic_text(runtime / "KILL", "Watchdog detected stalled monitor\n")
                    audit(runtime,datetime.now(UTC),'WATCHDOG_STALL',worker_pid=child.pid,heartbeat_age=age)
                    child.kill()
                    break
            if stop or (runtime/"STOP").exists():
                child.terminate()
            else:
                atomic_text(runtime / "KILL", "Child crashed or stalled; manual clearance required\n")
                audit(runtime,datetime.now(UTC),'WORKER_RESTART',worker_pid=child.pid,exit_code=child.poll())
            try:
                child.wait(timeout=12)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            if not stop and not (runtime/"STOP").exists():
                time.sleep(5)
        if inhibitor is not None:
            inhibitor.terminate()
            inhibitor.wait()
        if host_monitor is not None:
            host_monitor.terminate()
            host_monitor.wait(timeout=15)
