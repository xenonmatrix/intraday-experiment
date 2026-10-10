"""Small, date-authorized LIVE engine. All setup/tests use a fake transport."""
from dataclasses import replace
from datetime import datetime, timedelta
import json
import time
from pathlib import Path
from .engine import Engine
from .execution import verify_actual_order
from .live_policy import LivePolicy
from .models import Config, D, Mode, UTC, NY, Unsafe, PreSubmissionBlocked, bar_closed_after, dt, iso, down, valid_quote
from .risk import reconcile, eligible, size, TERMINAL
from .strategy import evaluate
from .store import atomic_text
from .unattended import Unattended, audit
from .broker import failure_details
from .probe import active as probe_active


class LiveEngine(Engine):
    def __init__(self, store, root, config=None, clock=None):
        self.store, self.root, self.mode = store, Path(root), Mode.LIVE
        self.root.mkdir(parents=True, exist_ok=True)
        self.policy = LivePolicy(self.root.parent.parent, clock)
        self.evaluate = evaluate
        if self.policy.strategy == 'orb':
            from .orb import evaluate as plain_orb
            self.evaluate = plain_orb
        self.unattended = Unattended(self.policy)
        self.cfg = replace(config or Config(), starting_cash=D("10"), position_cap=D("9"),
                           trade_risk=D("0.10"), daily_loss=D("0.50"), reserve=D("0"), active_positions=1)
        if self.policy.strategy == 'orb':
            self.cfg = replace(self.cfg, universe=tuple(s for s in self.cfg.universe if s in ('SPY','QQQ')))
            if not self.cfg.universe:
                raise Unsafe('Frozen ORB requires SPY/QQQ universe')
        self.kill = self.policy.runtime / "KILL"
        if store.get("mode") not in (None, "LIVE"):
            raise Unsafe("Mode/database mismatch")
        store.save("mode", "LIVE")
        self.account = store.get("account_number")
        self.last_quotes = {}
        self.proof = None
        self.mutation_acknowledged = False
        # PREPARED is provably never dispatched; DISPATCHING may have reached broker.
        with store.db:
            store.db.execute("UPDATE intents SET status='ABORTED' WHERE mode='LIVE' AND status='PREPARED'")
            store.db.execute("UPDATE intents SET status='AMBIGUOUS' WHERE mode='LIVE' AND status='DISPATCHING' AND broker_id IS NULL")
        if store.get("position") or store.unresolved():
            store.save("recovery_exit", True)
        self.rebuild_ledger()

    def now(self):
        return self.policy.clock()

    def rebuild_ledger(self):
        """Rebuild positions and daily P&L from unique actual executions, in fill order."""
        pos, cash, days = None, D("10"), {}
        rows = self.store.rows()
        for f in rows:
            if f.get("mode") != "LIVE":
                raise Unsafe("Non-live fill in live ledger")
            q, price, fee = D(f["quantity"]), D(f["fill_price"]), D(f["fee"])
            day = dt(f["timestamp"]).astimezone(NY).date().isoformat()
            daily = days.setdefault(day, dict(net="0", gross_losses="0", latched=False, trades=0, wins=0, losses=0, breakeven=0))
            if f["side"] == "buy":
                cash -= q*price+fee
                if pos is None:
                    pos = dict(symbol=f["ticker"], quantity="0", cost="0", entry_fees="0", entry="0", stop=f["stop"],
                               target="0", entered=f["timestamp"], time_exit=iso(dt(f["timestamp"])+timedelta(minutes=30)),
                               setup_id=f["setup_id"], entry_ref=f["ref_id"], cycle_pnl="0", reached_1r=False,
                               strategy=f.get('strategy', 'existing_orb_pullback_vwap'))
                if pos["symbol"] != f["ticker"] or pos["entry_ref"] != f["ref_id"]:
                    raise Unsafe("Multiple live positions or increasing an existing position")
                quantity = D(pos["quantity"])+q
                cost = D(pos["cost"])+q*price
                pos.update(quantity=str(quantity), cost=str(cost), entry_fees=str(D(pos["entry_fees"])+fee), entry=str(cost/quantity),
                           target=str(cost/quantity+2*(cost/quantity-D(pos["stop"]))))
            elif f["side"] == "sell":
                if not pos or pos["symbol"] != f["ticker"] or q > D(pos["quantity"]):
                    raise Unsafe("Unowned or oversized live sell fill")
                share = q/D(pos["quantity"])
                basis, entry_fee = D(pos["cost"])*share, D(pos["entry_fees"])*share
                pnl = q*price-basis-entry_fee-fee
                cash += q*price-fee
                daily["net"] = str(D(daily["net"])+pnl)
                daily["gross_losses"] = str(D(daily["gross_losses"])+max(D(0), -pnl))
                cycle = D(pos["cycle_pnl"])+pnl
                remaining = D(pos["quantity"])-q
                pos.update(quantity=str(remaining), cost=str(D(pos["cost"])-basis), entry_fees=str(D(pos["entry_fees"])-entry_fee), cycle_pnl=str(cycle))
                if remaining == 0:
                    daily["trades"] += 1
                    daily["wins" if cycle > 0 else "losses" if cycle < 0 else "breakeven"] += 1
                    pos = None
            else:
                raise Unsafe("Unsupported fill side")
        old_pos = self.store.get("position")
        if pos and old_pos and pos["entry_ref"] == old_pos.get("entry_ref"):
            pos["reached_1r"] = old_pos.get("reached_1r", False)
        self.store.put("cash", str(cash))
        self.store.put("position", pos)
        for day, d in days.items():
            d["latched"] = self.daily(day)["latched"] or D(d["gross_losses"]) >= D("0.50") or D(d["net"]) <= D("-0.50")
            self.store.put("daily:"+day, d)
        self.store.db.commit()

    def ingest(self, key, order):
        self.store.event(iso(self.now()),'LIVE_ORDER_OBSERVED',dict(intent_key=key,order=order))
        row = self.store.db.execute("SELECT * FROM intents WHERE intent_key=?", (key,)).fetchone()
        payload = json.loads(row["payload"])
        if order.get("market_hours") != "regular_hours" or order.get("type") != "market" or order.get("side") != payload["side"]:
            raise Unsafe("Unexpected actual order shape")
        verify_actual_order(self.store, key, order)
        for execution in order.get("executions") or []:
            existing = self.store.db.execute("SELECT payload FROM fills WHERE execution_id=?", (execution["id"],)).fetchone()
            if existing:
                continue
            pos = self.store.get("position")
            q, price, fee = D(execution["quantity"]), D(execution["price"]), D(execution["fees"])
            pnl, entry = "", str(price)
            if payload["side"] == "sell":
                if not pos or q > D(pos["quantity"]) or pos["symbol"] != payload["symbol"]:
                    raise Unsafe("Exit executions exceed owned position")
                entry = pos["entry"]
                pnl = str(q*(price-D(entry))-D(pos["entry_fees"])*q/D(pos["quantity"])-fee)
            fill = dict(timestamp=execution["timestamp"], ticker=payload["symbol"], side=payload["side"], quantity=str(q),
                        fill_price=str(price), fee=str(fee), ref_id=row["ref_id"], entry_price=entry,
                        exit_price=str(price) if payload["side"] == "sell" else "", position_value=str(q*price),
                        setup="LIVE:"+payload.get('strategy', 'existing_orb_pullback_vwap'),
                        strategy=payload.get('strategy', 'existing_orb_pullback_vwap'), setup_id=payload["setup_id"], stop=payload["stop"],
                        stop_condition=payload.get("reason", payload["stop"]), target_condition=payload["target"],
                        realized_pnl=pnl, account_value="", mode="LIVE")
            # Fill is source of truth. Restart rebuilds materialized accounting if
            # interrupted between this commit and the deterministic rebuild.
            with self.store.db:
                self.store.db.execute("INSERT INTO fills VALUES(?,?,?)", (execution["id"], row["ref_id"], json.dumps(fill)))
            self.rebuild_ledger()
        pos = self.store.get("position")
        if pos:
            planned = D(pos["quantity"])*max(D(0), D(pos["entry"])-D(pos["stop"]))+D(pos["entry_fees"])+self.cfg.fee_buffer
            if D(pos["cost"]) > D('9') or D(pos["cost"])+D(pos["entry_fees"]) > D("10") or planned > D("0.10"):
                self.request_liquidation("Actual fill exceeded capital/planned-risk estimate")

    async def sync(self, broker):
        # Revisit even terminal orders: a crash may have occurred before fill materialization.
        for row in list(self.store.db.execute("SELECT * FROM intents WHERE mode='LIVE' ORDER BY rowid")):
            if row["broker_id"]:
                # Terminal, accounted orders need not cost one API request on each 2s cycle.
                accounted = self.store.get("accounted:"+row["broker_id"])
                if row["status"] in ("FILLED", "CANCELLED", "REJECTED") and accounted:
                    continue
                actual = await broker.order(self.account, row["broker_id"])
                self.ingest(row["intent_key"], actual)
                if actual["state"] not in TERMINAL:
                    self.store.save("working:"+row["broker_id"], actual)
                if actual["state"] in TERMINAL:
                    self.store.save("accounted:"+row["broker_id"], True)
        if any(r["status"] == "AMBIGUOUS" and not r["broker_id"] for r in self.store.unresolved()):
            raise Unsafe("Ambiguous submission without broker ID; manual reconciliation required, no retries")

    async def cancel_working_entry(self, broker):
        """Cancel only a known experiment BUY; verify final status on later polls."""
        now = self.now()
        for row in self.store.unresolved():
            p = json.loads(row["payload"])
            actual = self.store.get("working:"+str(row["broker_id"]))
            if p["side"] != "buy" or not actual or actual["state"] in TERMINAL:
                continue
            age = (now-dt(p["checked_at"])).total_seconds()
            emergency = self.store.get("recovery_exit") or any((self.policy.runtime/f).exists() for f in ("KILL","LIQUIDATE"))
            pos=self.store.get("position")
            if pos:
                try:
                    bid,ask=valid_quote(self.last_quotes[pos["symbol"]],now,self.cfg)
                    emergency = emergency or D(pos["quantity"])*ask >= 10 or bid <= D(pos["stop"]) or bid >= D(pos["target"])
                except (Unsafe,KeyError):
                    emergency=True
            if not emergency and age < 30 and now < self.policy.session.close-timedelta(minutes=15):
                continue
            self.policy.authorize("sell",now)
            key = "cancellation:"+row["broker_id"]
            if self.store.get(key):
                continue  # Never retry a cancellation with unknown outcome.
            self.store.save(key,{"state":"DISPATCHING","at":iso(now)})
            try:
                result = await broker.cancel_owned({"account_number":self.account,"order_id":row["broker_id"]})
                self.store.save(key,{"state":"ACKNOWLEDGED","response":result,"at":iso(now)})
            except BaseException:
                self.store.save(key,{"state":"AMBIGUOUS","at":iso(now)})
                raise
            self.request_liquidation("Entry did not complete promptly; cancel then reconcile")

    def cancel_guard(self,args):
        self.policy.authorize("sell")
        if set(args) != {"account_number","order_id"} or args["account_number"] != self.account:
            raise Unsafe("Invalid cancellation account/parameters")
        row = self.store.db.execute("SELECT * FROM intents WHERE broker_id=?",(args["order_id"],)).fetchone()
        if not row or row["status"] in ("FILLED","CANCELLED","REJECTED","ABORTED") or json.loads(row["payload"])["side"] != "buy":
            raise Unsafe("Cancellation limited to this experiment's working entry")
        if self.store.get("cancellation:"+args["order_id"],{}).get("state") != "DISPATCHING":
            raise Unsafe("Cancellation is not journaled for one dispatch")

    def request_liquidation(self, reason):
        atomic_text(self.policy.runtime/"ENTRIES_DISABLED", reason+"\n")
        atomic_text(self.policy.runtime/"LIQUIDATE", reason+"\n")
        self.store.event(iso(self.now()), "LIQUIDATION_REQUESTED", {"reason":reason})

    def check_account(self, snapshot, now):
        pos = self.store.get("position")
        expected = {pos["symbol"]:pos["quantity"]} if pos else {}
        power = reconcile(snapshot, self.account, now, self.cfg, expected)
        for p in snapshot["positions"]:
            if D(p["quantity"]) and (D(p["shares_available_for_sells"]) != D(p["quantity"])):
                raise Unsafe("Experiment shares have unexplained holds")
        self.store.save("broker_snapshot", snapshot)
        self.unattended.health(now,True,snapshot_at=snapshot['observed_at'])
        self.store.event(iso(now),'LIVE_RECONCILIATION',snapshot)
        return power

    def write_guard(self, args):
        """Last in-process and transport gate, immediately before the one network call."""
        now = self.now()
        self.policy.authorize(args.get("side"), now)
        self.policy.validate_order_shape(args)
        if self.mode != Mode.LIVE or args.get("account_number") != self.account or args.get("symbol") not in self.cfg.universe:
            raise Unsafe("Invalid live account, universe or mode")
        row = self.store.db.execute("SELECT * FROM intents WHERE ref_id=?", (args["ref_id"],)).fetchone()
        if not row or row["status"] != "DISPATCHING":
            raise Unsafe("No uniquely dispatching intent")
        if not self.proof or self.proof["args"] != args or not 0 <= (now-dt(self.proof["at"])).total_seconds() <= 5:
            raise Unsafe("Live preflight expired or request changed")
        if args["side"] == "buy" and self.store.get("monitor_deadline_failed"):
            raise Unsafe("Monitor deadline failure")
        if args["side"] == "buy" and time.monotonic()-getattr(self,"tick_started",time.monotonic()) > self.cfg.heartbeat_timeout:
            self.store.save("monitor_deadline_failed",True)
            raise Unsafe("Live preflight exceeded monitoring deadline")
        if args["side"] == "buy" and self.daily(self.policy.day)["latched"]:
            raise Unsafe("Session loss latch")
        payload = json.loads(row["payload"])
        valid_quote(payload["quote"],now,self.cfg)
        power = self.check_account(self.store.get("broker_snapshot"),now)
        pos = self.store.get("position")
        if args["side"] == "buy":
            if power-D(args['dollar_amount']) < D('20'):
                raise Unsafe('Broker buying-power reserve below $20')
            if pos or D(args["dollar_amount"]) > min(D("10"),D(self.store.get("cash"))):
                raise Unsafe("No live top-ups or extra capital")
            risk = D(payload.get("planned_risk","999"))
            if not 0 < risk <= min(D(".10"),D(".50")-D(self.daily(self.policy.day)["gross_losses"])):
                raise Unsafe("Live planned risk exceeds allowance")
        elif not pos or args["symbol"] != pos["symbol"] or D(args["quantity"]) != D(pos["quantity"]):
            raise Unsafe("Exit does not match the owned experiment position")

    async def dispatch(self, broker, key, payload):
        args = payload["args"]
        self.policy.authorize(args["side"])
        self.policy.validate_order_shape(dict(args, ref_id="pending"))
        intent = self.store.intent(key, "LIVE", payload)
        if intent["status"] != "PREPARED":
            raise Unsafe("Intent already dispatched/aborted; never resubmit")
        if self.store.unresolved() and any(r["intent_key"] != key for r in self.store.unresolved()):
            raise Unsafe("Another unresolved intent blocks submission")
        wire = dict(args, ref_id=intent["ref_id"])
        self.proof = dict(args=wire, at=payload["checked_at"])
        with self.store.db:
            updated = self.store.db.execute("UPDATE intents SET status='DISPATCHING' WHERE intent_key=? AND status='PREPARED'", (key,)).rowcount
        if updated != 1:
            raise Unsafe("Concurrent/duplicate dispatch")
        try:
            self.write_guard(wire)
        except BaseException:
            with self.store.db:
                self.store.db.execute("UPDATE intents SET status='ABORTED' WHERE intent_key=?",(key,))
            self.proof=None
            raise
        try:
            response = await broker.submit(wire)
            order_id = (response.get("order") or {}).get("id")
            if not order_id:
                raise Unsafe("Submission response lacks broker order ID")
            with self.store.db:
                self.store.db.execute("UPDATE intents SET broker_id=?, status='SUBMITTED' WHERE intent_key=?", (order_id,key))
            self.mutation_acknowledged = True
            # Acknowledgement is not a fill. Always query after submission.
            actual = await broker.order(self.account, order_id)
            self.ingest(key, actual)
            self.store.save("last_submission", {"at":iso(self.now()), "order_id":order_id, "state":actual["state"]})
            after=await broker.snapshot(self.account)
            self.store.save("broker_snapshot",after)
            with self.store.db:
                for f in self.store.db.execute("SELECT execution_id,payload FROM fills WHERE ref_id=?",(intent["ref_id"],)).fetchall():
                    value=json.loads(f["payload"])
                    value.update(account_value=after["portfolio"]["total_value"],account_value_asof=after["observed_at"])
                    self.store.db.execute("UPDATE fills SET payload=? WHERE execution_id=?",(json.dumps(value),f["execution_id"]))
        except PreSubmissionBlocked:
            with self.store.db:
                self.store.db.execute("UPDATE intents SET status='ABORTED' WHERE intent_key=?",(key,))
            raise
        except BaseException:
            with self.store.db:
                self.store.db.execute("UPDATE intents SET status='AMBIGUOUS' WHERE intent_key=?", (key,))
            raise
        finally:
            self.proof = None

    async def exit_if_needed(self, broker, session, snapshot, quotes, candidates):
        pos = self.store.get("position")
        if not pos:
            return False
        now = self.now()
        if not session or not session.contains(now):
            return False
        self.policy.authorize("sell", now)
        if self.store.unresolved():
            raise Unsafe("Awaiting known terminal order state before any exit")
        self.check_account(snapshot, now)
        bid, ask = valid_quote(quotes[pos["symbol"]], now, self.cfg)
        exposure = D(pos["quantity"])*ask
        risk = D(pos["quantity"])*max(D(0), D(pos["entry"])-D(pos["stop"]))+D(pos["entry_fees"])+self.cfg.fee_buffer
        unrealized = D(pos["quantity"])*(bid-D(pos["entry"]))-D(pos["entry_fees"])-self.cfg.fee_buffer
        if D(self.daily(session.day)["net"])+unrealized <= D("-.50"):
            self.latch(session.day,"Realized plus unrealized session loss")
        if bid >= D(pos["entry"])+(D(pos["entry"])-D(pos["stop"])):
            pos["reached_1r"] = True
            self.store.save("position",pos)
        reason = None
        if exposure >= D("10") or risk > D(".10"):
            reason = "EXPOSURE_OR_FILL_RISK_LIMIT"
            self.request_liquidation(reason)
        elif any((self.policy.runtime/f).exists() for f in ("LIQUIDATE","KILL")):
            reason = "EMERGENCY"
        elif self.store.get("recovery_exit"):
            reason = "RESTART_RECOVERY"
        elif self.daily(session.day)["latched"]:
            reason = "DAILY_LOSS"
        elif now >= session.close-timedelta(minutes=15):
            reason = "SESSION_CLOSE"
        elif bid <= D(pos["stop"]):
            reason = "INVALIDATION"
        elif bid >= D(pos["target"]):
            reason = "TARGET"
        elif candidates.get(pos["symbol"],{}).get("below_vwap") and bar_closed_after(candidates.get(pos["symbol"],{}).get("last_bar"), pos.get("entered")):
            reason = "VWAP"
        elif now >= dt(pos["time_exit"]) and (pos.get('strategy') == 'orb' or not pos["reached_1r"]):
            reason = "TIME"
        if not reason:
            return False
        args = dict(account_number=self.account,symbol=pos["symbol"],side="sell",quantity=pos["quantity"],type="market",market_hours="regular_hours",time_in_force="gfd")
        review = await broker.review_order(args)
        self.store.event(iso(now),"LIVE_EXIT_REVIEW",review)
        if (review.get("order_checks") != {} or review.get("symbol") != pos["symbol"] or review.get("side") != "sell"
                or review.get("type") != "market" or D(review.get("quantity","-1")) != D(pos["quantity"])):
            raise Unsafe("Exit review warning; no bypass")
        # Review may take time; refresh both holdings and quote before the sell.
        fresh = await broker.snapshot(self.account)
        freshq = await broker.quotes([pos["symbol"]])
        checked = self.now()
        self.check_account(fresh,checked)
        valid_quote(freshq[pos["symbol"]],checked,self.cfg)
        prefix=f"exit:{pos['entry_ref']}:{pos['quantity']}:"
        attempt=self.store.db.execute("SELECT count(*) FROM intents WHERE intent_key LIKE ?",(prefix+"%",)).fetchone()[0]
        key=prefix+str(attempt)
        payload = dict(symbol=pos["symbol"],side="sell",strategy=pos.get('strategy','existing_orb_pullback_vwap'),quantity=pos["quantity"],setup_id=pos["setup_id"],stop=pos["stop"],target=pos["target"],reason=reason,args=args,checked_at=iso(checked),quote=freshq[pos["symbol"]],review=review)
        await self.dispatch(broker,key,payload)
        return True

    async def tick(self, broker, now, session, snapshot, quotes, bars, tradability):
        self.tick_started=time.monotonic()
        self.last_quotes = quotes
        reconciled = False
        try:
            # Reconcile actual executions before any further action, including exits.
            try:
                await self.sync(broker)
            except Exception as exc:
                if not getattr(broker,'cycle_error',None):
                    self.unattended.failure(self.store,self.now(),', '.join(failure_details(exc)))
                raise
            if self.store.unresolved():
                self.unattended.health(self.now(),False,reason='Outstanding order reconciliation pending')
                await self.cancel_working_entry(broker)
                raise Unsafe("Outstanding order: monitor status, no new order")
            try:
                # This is the single LIVE observation. External snapshots cannot
                # replace fresh holdings/orders, including after syncing fills.
                snapshot = await getattr(broker,'observe',broker.snapshot)(self.account)
                power = self.check_account(snapshot,self.now())
                reconciled = True
            except Exception as exc:
                if not getattr(broker,'cycle_error',None):
                    self.unattended.failure(self.store,self.now(),', '.join(failure_details(exc)))
                raise
            # Slow reconciliation must not age the entry/exit quote. Keep the
            # existing five-second freshness limit and read quotes last.
            held=self.store.get('position')
            symbols=[held['symbol']] if held else self.cfg.universe
            probing=probe_active(self.policy.runtime,self.now())
            if probing and not held: symbols=['SPY','QQQ']
            quotes=await broker.quotes(symbols) if probing or (session and session.contains(self.now())) else {}
            self.last_quotes=quotes
            # Liquidation must not wait for candidate evaluation or entry data.
            if self.unattended.liquidation_due(self.now()) or self.store.get('recovery_exit'):
                if await self.exit_if_needed(broker,session,snapshot,quotes,{}):
                    self.block(self.now(),'Priority liquidation; actual broker status recorded')
                    return
            candidates = {}
            for symbol in self.cfg.universe:
                try:
                    c = self.evaluate(symbol,bars.get(symbol,[]),session,self.now()) if session else {}
                    if c.get("last_bar"):
                        last = next(b for b in bars[symbol] if iso(dt(b["begins_at"])) == c["last_bar"])
                        c["below_vwap"] = D(last["close_price"]) < D(c["vwap"])
                    candidates[symbol] = c
                    self.store.save("candidate:"+symbol,c)
                except (Unsafe,KeyError,ValueError):
                    candidates[symbol] = {"state":"DATA_INVALID"}
                    self.store.save("candidate:"+symbol,candidates[symbol])
            self.store.event(iso(self.now()),'LIVE_OBSERVATION',dict(quotes=quotes,candidates=candidates))
            if await self.exit_if_needed(broker,session,snapshot,quotes,candidates):
                self.block(self.now(),"Exit submitted; verify broker before any next action")
                return
            self.policy.authorize("buy")
            if getattr(broker, 'diagnostic_runtime', None) is not None:
                from .host_health import require_host
                require_host(broker.diagnostic_runtime, self.now())
            if self.store.get("position"):
                raise Unsafe("One live position already open")
            daily = self.daily(self.policy.day)
            if daily["latched"] or D(daily["gross_losses"]) >= D(".50"):
                self.latch(self.policy.day,"Cumulative realized losses")
                raise Unsafe("Session loss latch")
            if any(c.get("state") == "DATA_INVALID" for c in candidates.values()):
                raise Unsafe("Strategy market data incomplete")
            ranked = []
            capital = min(D("10"),D(self.store.get("cash")))
            power = max(D(0), power-D('20'))  # Account reserve is separate from the $10 ledger.
            for symbol in self.cfg.universe:
                q = quotes[symbol]
                bid,ask = valid_quote(q,self.now(),self.cfg)
                c = candidates[symbol]
                if c.get("state") != "ARMED" or not eligible(tradability.get(symbol,{})):
                    continue
                if self.store.db.execute("SELECT 1 FROM intents WHERE intent_key=?",("entry:"+c["setup_id"],)).fetchone():
                    continue
                try:
                    qty,price,notional = size(c,q,power,capital,daily,self.now(),self.cfg)
                    dollars = down(notional,"0.01")
                    if dollars < 1 or dollars > 9:
                        continue
                    score = (ask-bid+2*price/10000)/(D(c["trigger"])-D(c["invalidation"]))
                    ranked.append((score,-D(c["session_dollars"]),symbol,c,dollars))
                except Unsafe:
                    continue
            if not ranked:
                self.store.save("gate",{"at":iso(self.now()),"entries_allowed":True,"reason":"No qualified live setup"})
                return
            _,_,symbol,c,dollars = sorted(ranked)[0]
            args = dict(account_number=self.account,symbol=symbol,side="buy",dollar_amount=str(dollars),type="market",market_hours="regular_hours",time_in_force="gfd")
            review = await broker.review_order(args)
            self.store.event(iso(self.now()),"LIVE_ENTRY_REVIEW",review)
            if review.get("order_checks") != {} or review.get("symbol") != symbol or review.get("side") != "buy" or review.get("type") != "market":
                raise Unsafe("Broker review warning/mismatch")
            if D(review.get("dollar_amount","-1")) != dollars:
                raise Unsafe("Broker review amount mismatch")
            fresh = await broker.snapshot(self.account)
            t = await broker.tradability(self.account,[symbol])
            # Refresh strategy too: a signal cannot survive an invalidating completed bar.
            freshbars = await broker.bars([symbol],session,self.now())
            q = (await broker.quotes([symbol]))[symbol]
            checked = self.now()
            self.policy.authorize("buy",checked)
            power = self.check_account(fresh,checked)
            if not eligible(t.get(symbol,{})):
                raise Unsafe("Symbol eligibility changed")
            current = self.evaluate(symbol,freshbars[symbol],session,checked)
            if current.get("state") != "ARMED" or current.get("setup_id") != c["setup_id"]:
                raise Unsafe("Setup changed after review")
            _,price,max_notional = size(current,q,max(D(0),power-D('20')),capital,daily,checked,self.cfg)
            if dollars > down(max_notional,"0.01"):
                raise Unsafe("Reviewed amount no longer satisfies risk/exposure gates")
            estimate = dollars/price
            risk = estimate*(price-D(c["invalidation"])+price/10000)+2*self.cfg.fee_buffer
            if dollars > D("9") or dollars > capital or risk > min(D(".10"),D(".50")-D(daily["gross_losses"])):
                raise Unsafe("Final live capital or risk cap")
            payload = dict(symbol=symbol,side="buy",strategy=self.policy.strategy,dollar_amount=str(dollars),setup_id=c["setup_id"],stop=c["invalidation"],target=c["target"],args=args,checked_at=iso(checked),planned_risk=str(risk),review=review,quote=q)
            await self.dispatch(broker,"entry:"+c["setup_id"],payload)
            self.block(self.now(),"Live entry submitted; actual status recorded")
        except Exception as exc:
            if reconciled and not getattr(broker,'cycle_error',None) and not isinstance(exc,(Unsafe,KeyError,ValueError)):
                self.unattended.failure(self.store,self.now(),type(exc).__name__)
            self.block(self.now(),str(exc) if isinstance(exc,(Unsafe,KeyError,ValueError)) else type(exc).__name__)
        finally:
            self.report(self.now(),session)

    def report(self, now, session):
        pos = self.store.get("position")
        snapshot = self.store.get("broker_snapshot",{})
        cash = D(self.store.get("cash","10"))
        exposure = None if pos else D(0)
        liquidation_value = None if pos else D(0)
        if pos:
            try:
                bid,ask = valid_quote(self.last_quotes[pos["symbol"]],now,self.cfg)
                exposure = D(pos["quantity"])*ask
                liquidation_value = D(pos["quantity"])*bid
            except (Unsafe,KeyError):
                pass
        try:
            self.policy.approval()
            approval = "ARMED_FOR_"+self.policy.day
        except Unsafe:
            approval = "EXPIRED" if now >= self.policy.session.close else "DISARMED"
        benchmark=None
        if approval != "DISARMED" and session and session.day==self.policy.day and session.contains(now):
            try:
                bid,ask=valid_quote(self.last_quotes["SPY"],now,self.cfg)
                price=(bid+ask)/2
                baseline=self.store.get("benchmark") or {"start":str(price),"started":iso(now)}
                self.store.save("benchmark",baseline)
                benchmark={"price_return":str(price/D(baseline["start"])-1),"start":baseline["started"],"asof":iso(now)}
                self.store.save("benchmark_mark",benchmark)
            except (KeyError,Unsafe):
                pass
        status = dict(at=iso(now),mode="LIVE",authorization=approval,session_date=self.policy.day,strategy=self.policy.strategy,
                      gate=self.store.get("gate",{}),position=pos,exposure=str(exposure) if exposure is not None else None,
                      capital_envelope="10.00",entry_notional_ceiling="9.00",cash=str(cash),daily=self.daily(self.policy.day),
                      account_value=snapshot.get("portfolio",{}).get("total_value"),last_reconciliation=snapshot.get("observed_at"),
                      unresolved_orders=len(self.store.unresolved()),live_session_open=bool(session and session.contains(now) and session.day == self.policy.day))
        status["spy_benchmark"]=benchmark or self.store.get("benchmark_mark")
        status['unattended'] = {p.stem:json.loads(p.read_text()) for p in self.unattended.root.glob('*.json')}
        status['connectivity_failures'] = self.store.get('unattended_failures:'+self.policy.day,0)
        status["experiment_equity"]=str(cash+liquidation_value) if liquidation_value is not None else None
        status["experiment_pnl"]=str(cash+liquidation_value-D("10")) if liquidation_value is not None else None
        self.store.export_csv(self.root/"trades.csv")
        self.store.export_csv(self.root.parent.parent/"trades.csv")
        atomic_text(self.root/"status.json",json.dumps(status,indent=2))
        atomic_text(self.root/"experiment.md","# LIVE experiment status\n\n```json\n"+json.dumps(status,indent=2)+"\n```\n\nOnly verified actual executions appear in trades.csv.\n")
        if (self.policy.runtime/'READ_ONLY_SESSION.json').exists():
            from .session_report import update
            update(self,status,now)
