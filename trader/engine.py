import json
from datetime import datetime, timedelta
from pathlib import Path
from .models import Config, D, Mode, UTC, NY, Unsafe, bar_closed_after, dt, iso, valid_quote
from .risk import reconcile, eligible, size
from .strategy import evaluate
from .store import atomic_text


class Engine:
    def __init__(self, store, root, mode=Mode.PAPER, config=None):
        self.store, self.root, self.mode = store, Path(root), Mode(mode)
        self.cfg = config or Config()
        if self.mode == Mode.LIVE:
            raise Unsafe("Base engine is PAPER/READ_ONLY; use the date-authorized LiveEngine")
        self.root.mkdir(parents=True, exist_ok=True)
        self.kill = self.root.parent / "KILL"
        self.account = store.get("account_number")
        if store.get("mode") not in (None, self.mode.value):
            raise Unsafe("Mode/database mismatch")
        store.save("mode", self.mode.value)
        store.recover()
        if store.get("position"):
            store.save("recovery_exit", True)
        if store.get("cash") is None:
            store.save("cash", str(self.cfg.starting_cash))
        self.last_quotes = {}

    def daily(self, day):
        return self.store.get("daily:"+day, dict(net="0", gross_losses="0", latched=False, trades=0, wins=0, losses=0, breakeven=0))

    def latch(self, day, reason):
        d = self.daily(day)
        d["latched"] = True
        d["reason"] = reason
        self.store.save("daily:"+day, d)

    def block(self, now, reason):
        self.store.save("gate", dict(at=iso(now), entries_allowed=False, reason=reason))
        self.store.event(iso(now), "ENTRY_BLOCKED", {"reason": reason})

    def entry_allowed(self, day):
        stopped=any((self.root.parent/f).exists() for f in ("KILL","ENTRIES_DISABLED","LIQUIDATE","STOP"))
        return not stopped and not self.daily(day)["latched"] and not self.store.unresolved() and not self.store.get("position")

    def reconcile_paper_ledger(self):
        """Verify materialized cash/holdings against immutable committed fills."""
        cash, holdings = self.cfg.starting_cash, {}
        for fill in self.store.rows():
            qty, price = D(fill["quantity"]), D(fill["entry_price"] if fill["side"] == "buy" else fill["exit_price"])
            sign = 1 if fill["side"] == "buy" else -1
            holdings[fill["ticker"]] = holdings.get(fill["ticker"], D(0))+sign*qty
            cash -= sign*qty*price+D(fill["fee"])
            if holdings[fill["ticker"]] < 0:
                raise Unsafe("Paper ledger contains short exposure")
        holdings = {s:q for s,q in holdings.items() if q}
        pos = self.store.get("position")
        expected = {pos["symbol"]:D(pos["quantity"])} if pos else {}
        if cash != D(self.store.get("cash")) or holdings != expected:
            raise Unsafe("Paper ledger/materialized state mismatch")

    def paper_fill(self, key, payload, now):
        if self.mode != Mode.PAPER:
            raise Unsafe("Simulated fill requires PAPER")
        intent = self.store.intent(key, self.mode.value, payload)
        if intent["status"] == "FILLED":
            return False  # Same intended order has already been applied.
        if intent["status"] != "PREPARED":
            raise Unsafe("Unresolved or aborted intent")
        day = now.astimezone(NY).date().isoformat()
        qty, price = D(payload["quantity"]), D(payload["price"])
        fee, value = self.cfg.fee_buffer, qty*price
        cash = D(self.store.get("cash"))
        pos = self.store.get("position")
        daily = self.daily(day)
        if qty <= 0 or price <= 0:
            raise Unsafe("Nonpositive simulated fill")
        if payload["side"] == "buy":
            stopped=any((self.root.parent/f).exists() for f in ("KILL","ENTRIES_DISABLED","LIQUIDATE","STOP"))
            if stopped or daily["latched"] or pos or value > self.cfg.position_cap or cash-value-fee < self.cfg.reserve:
                raise Unsafe("Final simulated entry gate")
            unresolved = [r for r in self.store.unresolved() if r["intent_key"] != key]
            if unresolved:
                raise Unsafe("Other unresolved intent")
            risk = qty*(price-D(payload["stop"])+price*self.cfg.slip_bps/D(10000)) + 2*fee
            if risk > min(self.cfg.trade_risk, self.cfg.daily_loss-D(daily["gross_losses"])):
                raise Unsafe("Final planned-loss gate")
            cash -= value+fee
            pos = dict(symbol=payload["symbol"], quantity=str(qty), entry=str(price), stop=payload["stop"],
                       target=str(price+2*(price-D(payload["stop"]))), entered=iso(now),
                       time_exit=iso(now+timedelta(minutes=30)), reached_1r=False,
                       entry_fee=str(fee), setup_id=payload["setup_id"])
            pnl = ""
            entry, exit_price = str(price), ""
        elif payload["side"] == "sell":
            if not pos or pos["symbol"] != payload["symbol"] or qty != D(pos["quantity"]):
                raise Unsafe("Sell must match verified simulated holdings")
            entry, exit_price = pos["entry"], str(price)
            pnl = qty*(price-D(entry))-D(pos["entry_fee"])-fee
            cash += value-fee
            daily["net"] = str(D(daily["net"])+pnl)
            daily["gross_losses"] = str(D(daily["gross_losses"])+max(D(0), -pnl))
            daily["trades"] += 1
            daily["wins" if pnl > 0 else "losses" if pnl < 0 else "breakeven"] += 1
            if D(daily["gross_losses"]) >= self.cfg.daily_loss or D(daily["net"]) <= -self.cfg.daily_loss:
                daily["latched"] = True
            pos = None
        else:
            raise Unsafe("Invalid side")
        account_value = cash + (D(pos["quantity"])*price if pos else 0)
        fill = dict(timestamp=iso(now), ticker=payload["symbol"], side=payload["side"], quantity=str(qty), entry_price=entry,
                    exit_price=exit_price, position_value=str(value), setup="PAPER:OR15_PULLBACK_VWAP",
                    stop_condition=payload.get("stop", payload.get("reason", "")), target_condition=payload.get("target", ""),
                    realized_pnl=str(pnl), account_value=str(account_value), fee=str(fee), ref_id=intent["ref_id"], mode="PAPER")
        # One durable transaction contains fill, intent, cash, position and daily P&L.
        # Crash before commit => none occurred; crash after commit => all occurred.
        with self.store.db:
            self.store.db.execute("INSERT INTO fills VALUES(?,?,?)", ("paper:"+intent["ref_id"], intent["ref_id"], json.dumps(fill)))
            self.store.db.execute("UPDATE intents SET status='FILLED', filled_quantity=? WHERE intent_key=?", (str(qty), key))
            self.store.put("cash", str(cash))
            self.store.put("position", pos)
            self.store.put("daily:"+day, daily)
            self.store.put("recovery_exit", False)
        return True

    def manage_exit(self, now, session, quotes, candidates, broker_reachable):
        pos = self.store.get("position")
        if not pos or self.mode != Mode.PAPER or not broker_reachable or not session or not session.contains(now):
            return
        self.reconcile_paper_ledger()
        q = quotes.get(pos["symbol"])
        if not q:
            raise Unsafe("Held-position quote missing")
        bid, _ = valid_quote(q, now, self.cfg)
        unrealized = D(pos["quantity"])*(bid-D(pos["entry"]))-D(pos["entry_fee"])-self.cfg.fee_buffer
        daily = self.daily(session.day)
        if D(daily["net"])+unrealized <= -self.cfg.daily_loss:
            self.latch(session.day, "Realized plus unrealized daily loss")
        if bid >= D(pos["entry"])+(D(pos["entry"])-D(pos["stop"])):
            pos["reached_1r"] = True
            self.store.save("position", pos)
        c = candidates.get(pos["symbol"], {})
        reason = None
        if self.kill.exists() or (self.root.parent/"LIQUIDATE").exists():
            reason = "KILL"
        elif self.store.get("recovery_exit"):
            reason = "RESTART_RECOVERY"
        elif self.daily(session.day)["latched"]:
            reason = "DAILY_LOSS"
        elif dt(pos["entered"]).astimezone(NY).date().isoformat() != session.day:
            reason = "OVERNIGHT_EXCEPTION"
        elif now >= session.close-timedelta(minutes=15):
            reason = "SESSION_CLOSE"
        elif bid <= D(pos["stop"]):
            reason = "INVALIDATION"
        elif bid >= D(pos["target"]):
            reason = "TARGET"
        elif c.get("below_vwap") and bar_closed_after(c.get("last_bar"), pos.get("entered")):
            reason = "VWAP"
        elif now >= dt(pos["time_exit"]) and not pos["reached_1r"]:
            reason = "TIME"
        if reason:
            price = bid*(1-self.cfg.slip_bps/D(10000))
            self.paper_fill("exit:"+pos["setup_id"], dict(symbol=pos["symbol"], side="sell", quantity=pos["quantity"], price=str(price), reason=reason), now)
            self.store.event(iso(now), "PAPER_EXIT", {"symbol": pos["symbol"], "reason": reason})

    async def tick(self, broker, now, session, snapshot, quotes, bars, tradability):
        self.last_quotes = quotes
        if snapshot is not None:
            self.store.save("broker_snapshot", snapshot)
        candidates, errors = {}, []
        if session and session.contains(now):
            for symbol in self.cfg.universe:
                try:
                    c = evaluate(symbol, bars.get(symbol, []), session, now)
                    if c["last_bar"]:
                        last = next(b for b in bars[symbol] if iso(dt(b["begins_at"])) == c["last_bar"])
                        c["below_vwap"] = D(last["close_price"]) < D(c["vwap"])
                    candidates[symbol] = c
                    self.store.save("candidate:"+symbol, c)
                except (Unsafe, KeyError, ValueError) as exc:
                    self.store.save("candidate:"+symbol, {"state": "DATA_INVALID", "reason": str(exc)})
                    errors.append(f"{symbol}: invalid bars")
        # Exits evaluated before entry account gating; loss/kill does not disable exits.
        try:
            self.manage_exit(now, session, quotes, candidates, snapshot is not None)
        except (Unsafe, KeyError, ValueError) as exc:
            errors.append(str(exc))
        try:
            if self.mode == Mode.READ_ONLY:
                raise Unsafe("READ_ONLY observation; all execution disabled")
            if not session or not session.contains(now):
                raise Unsafe("Outside regular exchange session")
            if snapshot is None or self.account is None:
                raise Unsafe("Robinhood unavailable/account unpinned")
            self.reconcile_paper_ledger()
            power = reconcile(snapshot, self.account, now, self.cfg)
            self.store.save("last_reconciliation", {"at": iso(now), "buying_power": str(power), "account_value": snapshot["portfolio"]["total_value"]})
            if errors:
                raise Unsafe("; ".join(errors))
            if self.kill.exists():
                raise Unsafe("Emergency kill switch")
            if self.store.unresolved():
                raise Unsafe("Unresolved order intent")
            if not self.entry_allowed(session.day):
                raise Unsafe("Position occupied or daily loss latch")
            if not session.entry_window(now):
                raise Unsafe("Outside entry window")
            ranked = []
            # Rank identically: friction/stop distance, then session liquidity, then ticker.
            # SPY/QQQ receive no special entry preference.
            for symbol in self.cfg.universe:
                q = quotes.get(symbol)
                if not q:
                    raise Unsafe("Universe quote missing")
                valid_quote(q, now, self.cfg)
                c = candidates.get(symbol, {})
                if c.get("state") != "ARMED" or not eligible(tradability.get(symbol, {})):
                    continue
                if self.store.db.execute("SELECT 1 FROM intents WHERE intent_key=?", ("entry:"+c["setup_id"],)).fetchone():
                    continue
                try:
                    qty, price, dollars = size(c, q, power, D(self.store.get("cash")), self.daily(session.day), now, self.cfg)
                    score = (D(q["ask_price"])-D(q["bid_price"])+2*price*self.cfg.slip_bps/D(10000))/(D(c["trigger"])-D(c["invalidation"]))
                    ranked.append((score, -D(c["session_dollars"]), symbol, c, qty, price, dollars))
                except Unsafe:
                    continue
            self.store.save("gate", {"at": iso(now), "entries_allowed": True, "reason": "No qualifying setup" if not ranked else "Review pending"})
            if ranked:
                _, _, symbol, c, qty, price, dollars = sorted(ranked)[0]
                review = await broker.review(self.account, symbol, dollars.quantize(D("0.01")))
                self.store.event(iso(now), "BROKER_REVIEW", review)
                if review.get("order_checks") != {}:
                    raise Unsafe("Broker review alerts or missing checks")
                if review.get("symbol") != symbol or review.get("side") != "buy" or review.get("type") != "market":
                    raise Unsafe("Review mismatch")
                # Account + quote recheck AFTER review; no stale signal bypass.
                fresh = await broker.snapshot(self.account)
                fresh_quotes = await broker.quotes([symbol])
                fresh_tradability = await broker.tradability(self.account, [symbol])
                checked = broker.now() if hasattr(broker, "now") else datetime.now(UTC)
                if not session.entry_window(checked) or self.kill.exists() or not self.entry_allowed(session.day):
                    raise Unsafe("Final session/kill/position gate")
                power = reconcile(fresh, self.account, checked, self.cfg)
                if not eligible(fresh_tradability.get(symbol, {})):
                    raise Unsafe("Final symbol eligibility changed")
                qty2, price2, dollars2 = size(c, fresh_quotes[symbol], power, D(self.store.get("cash")), self.daily(session.day), checked, self.cfg)
                if dollars2 > dollars:
                    qty2 = min(qty2, qty)  # never increase reviewed risk/notional
                if qty2*price2 > dollars:
                    from .models import down
                    qty2 = down(dollars/price2)
                payload = dict(symbol=symbol, side="buy", quantity=str(qty2), price=str(price2), stop=c["invalidation"], target=c["target"], setup_id=c["setup_id"])
                self.paper_fill("entry:"+c["setup_id"], payload, checked)
                self.store.save("gate", {"at": iso(checked), "entries_allowed": False, "reason": "Simulated position open"})
        except (Unsafe, KeyError, ValueError, TimeoutError, ConnectionError) as exc:
            self.block(now, str(exc))
        self.report(now, session)

    def report(self, now, session):
        pos = self.store.get("position")
        cash = D(self.store.get("cash"))
        value, marked = cash, not bool(pos)
        if pos and pos["symbol"] in self.last_quotes:
            try:
                bid, _ = valid_quote(self.last_quotes[pos["symbol"]], now, self.cfg)
                value += D(pos["quantity"])*bid
                marked = True
            except Unsafe:
                pass
        if not marked:
            value = None
        if session and session.contains(now) and "SPY" in self.last_quotes:
            try:
                bid, ask = valid_quote(self.last_quotes["SPY"], now, self.cfg)
                mark = (bid+ask)/2
                if not self.store.get("benchmark"):
                    self.store.save("benchmark", {"start": str(mark), "started": iso(now)})
                self.store.save("benchmark_mark", {"price": str(mark), "at": iso(now)})
            except Unsafe:
                pass
        day = now.astimezone(NY).date().isoformat()
        daily = self.daily(day)
        rows = self.store.rows()
        sells = [r for r in rows if r["side"] == "sell"]
        wins = sum(D(r["realized_pnl"]) > 0 for r in sells)
        losses = sum(D(r["realized_pnl"]) < 0 for r in sells)
        benchmark, bm = self.store.get("benchmark"), self.store.get("benchmark_mark")
        benchmark_text = "Not started (requires fresh regular-session quotes)"
        if benchmark and bm:
            benchmark_text = f"{(D(bm['price'])/D(benchmark['start'])-1)*100:.4f}% price return, {benchmark['started']} to {bm['at']} (no dividends)"
        gate = self.store.get("gate", {})
        text = f"""# Experiment runtime status — {self.mode.value}

Updated: {iso(now)}. LIVE submission: DISABLED.
Starting simulated account value: $100.00.
Current simulated account value: {('$'+format(value, '.6f')) if value is not None else 'UNKNOWN: no fresh position mark'}.
Simulated cash / buying power: ${cash:.6f}.
Daily realized P&L: ${D(daily['net']):.6f}.
Daily cumulative losing trades: ${D(daily['gross_losses']):.6f}.
Remaining daily planned loss budget: ${max(D(0), self.cfg.daily_loss-D(daily['gross_losses'])):.6f}.
Cumulative account P&L: {('$'+format(value-self.cfg.starting_cash, '.6f')) if value is not None else 'UNKNOWN'}.
Completed round trips: {len(sells)}. Wins: {wins}. Losses: {losses}. Breakeven: {len(sells)-wins-losses}.
Win rate: {format(100*wins/len(sells), '.2f')+'%' if sells else 'N/A'}.
SPY benchmark: {benchmark_text}.
Entry gate: {gate.get('reason', 'Awaiting observation')}.
Open simulated position: {json.dumps(pos)}.

All fills in this directory are hypothetical, priced at ask+1bp / bid-1bp with $0.01 cost per side.
No profitability or fill guarantee. Real account reconciliation is independent of simulated holdings.
Rules: long controlled universe only; $20 position cap; one active position (ceiling 3); $0.25 planned loss/trade;
$5 gross losing-trade cutoff; $20 reserve; regular sessions only; no leverage, shorting, options, crypto, transfers.
See project experiment.md and README.md for full rules, failure behavior and operational limitations.
"""
        atomic_text(self.root / "experiment.md", text)
        self.store.export_csv(self.root / "trades.csv")
        atomic_text(self.root / "status.json", json.dumps({"at": iso(now), "mode": self.mode.value, "live_enabled": False, "gate": gate, "position": pos, "daily": daily, "value": str(value) if value is not None else None}, indent=2))
