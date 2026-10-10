from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from enum import StrEnum
from zoneinfo import ZoneInfo

D = Decimal
UTC = timezone.utc
NY = ZoneInfo("America/New_York")


class Mode(StrEnum):
    READ_ONLY = "READ_ONLY"
    PAPER = "PAPER"
    LIVE = "LIVE"


class Unsafe(RuntimeError):
    pass


class PreSubmissionBlocked(Unsafe):
    """The transport guard failed before any network submission."""


def dt(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise Unsafe("Naive timestamp")
    return result.astimezone(UTC)


def iso(value):
    return value.astimezone(UTC).isoformat()


def decimal(value):
    result = D(str(value))
    if not result.is_finite():
        raise Unsafe("Non-finite number")
    return result


def down(value, places="0.000001"):
    return value.quantize(D(places), rounding=ROUND_DOWN)


@dataclass(frozen=True)
class Config:
    # Controlled, unleveraged, US-listed universe; all ranked by same rules.
    universe: tuple = ("AAPL", "AMD", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "QQQ", "SPY", "TSLA")
    starting_cash: D = D("100")
    position_cap: D = D("20")
    configured_ceiling: int = 3
    active_positions: int = 1
    trade_risk: D = D("0.25")
    daily_loss: D = D("5")
    reserve: D = D("20")
    max_spread: D = D("0.0005")
    min_session_dollars: D = D("10000000")
    quote_age: float = 5
    account_age: float = 15
    poll_seconds: float = 2
    call_timeout: float = 8
    heartbeat_timeout: float = 15
    slip_bps: D = D("1")
    fee_buffer: D = D("0.01")
    breakout_offset: D = D("0.01")
    stop_offset: D = D("0.01")
    target_r: D = D("2")
    max_hold_minutes: int = 30
    vwap_exit: bool = True


def bar_closed_after(bar_begins, entered, minutes=5):
    """True when that bar's close is strictly after the fill.

    A VWAP exit may use only a bar that finished after the position existed.
    The bar in progress at the fill, and every earlier bar, does not count.
    """
    if not bar_begins or not entered:
        return False
    return dt(bar_begins) + timedelta(minutes=minutes) > dt(entered)


def execution_identity(execution):
    """Economic identity of one fill. Extra broker fields are not part of it."""
    return (execution.get("id"), decimal(execution["quantity"]), decimal(execution["price"]), decimal(execution["fees"]))


def valid_quote(q, now, cfg):
    bid, ask = decimal(q["bid_price"]), decimal(q["ask_price"])
    if not q.get("has_traded") or q.get("state") != "active" or not (0 < bid <= ask):
        raise Unsafe("Invalid quote/book")
    for key in ("venue_bid_time", "venue_ask_time"):
        age = (now - dt(q[key])).total_seconds()
        if age < 0:
            raise Unsafe(f"Future quote: {key} ahead by {-age:.6f}s; verify host clock")
        if age > cfg.quote_age:
            raise Unsafe(f"Stale quote: {key} age {age:.6f}s exceeds {cfg.quote_age}s")
    return bid, ask
