"""Actual order verification. Guarded submission lives in LiveEngine."""
import json
from .models import D, Mode, Unsafe, decimal, execution_identity


def verify_actual_order(store, intent_key, order):
    """Idempotently record verified real fills, never infer a fill from acceptance.

    Kept separate from PAPER accounting. Caller must reconcile positions before
    another action. Unknown/inconsistent state remains AMBIGUOUS across restarts.
    """
    row = store.db.execute("SELECT * FROM intents WHERE intent_key=?", (intent_key,)).fetchone()
    if not row or row["mode"] != "LIVE":
        raise Unsafe("No matching live intent")
    p = json.loads(row["payload"])
    def ambiguous(reason):
        with store.db:
            store.db.execute("UPDATE intents SET status='AMBIGUOUS' WHERE intent_key=?", (intent_key,))
        raise Unsafe(reason)
    if not row["broker_id"] or row["broker_id"] != order.get("id"):
        ambiguous("Unknown broker order identity")
    if order.get("symbol") != p["symbol"] or order.get("side") != p["side"]:
        ambiguous("Order identity mismatch")
    executions = order.get("executions") or []
    ids = [x["id"] for x in executions]
    cumulative = decimal(order["cumulative_quantity"])
    if len(set(ids)) != len(ids) or sum((decimal(e["quantity"]) for e in executions), D(0)) != cumulative:
        ambiguous("Cumulative fill mismatch")
    if cumulative < decimal(row["filled_quantity"]) or cumulative < 0:
        ambiguous("Fill quantity regressed")
    if "quantity" in p and cumulative > decimal(p["quantity"]):
        ambiguous("Overfill")
    mapping = {"new": "SUBMITTED", "queued": "SUBMITTED", "confirmed": "SUBMITTED", "unconfirmed": "AMBIGUOUS", "partially_filled": "PARTIAL", "filled": "FILLED", "cancelled": "CANCELLED", "partially_filled_rest_cancelled": "CANCELLED", "rejected": "REJECTED", "failed": "REJECTED", "voided": "REJECTED", "pending_cancelled": "SUBMITTED"}
    status = mapping.get(order.get("state"), "AMBIGUOUS")
    if status == "FILLED" and (cumulative <= 0 or (order.get("quantity") is not None and cumulative != decimal(order["quantity"]))):
        ambiguous("Filled state without complete executions")
    with store.db:
        for e in executions:
            if decimal(e["quantity"]) <= 0 or decimal(e["price"]) <= 0 or decimal(e["fees"]) < 0:
                raise Unsafe("Invalid execution")
            existing = store.get("actual_execution:" + e["id"])
            value = dict(ref_id=row["ref_id"], execution=e)
            if existing and (existing.get("ref_id") != row["ref_id"] or execution_identity(existing["execution"]) != execution_identity(e)):
                raise Unsafe("Execution changed")
            if not existing:
                store.put("actual_execution:" + e["id"], value)
        store.db.execute("UPDATE intents SET status=?, filled_quantity=? WHERE intent_key=?", (status, str(cumulative), intent_key))
    if status == "AMBIGUOUS":
        raise Unsafe("Ambiguous order state")
    return status


async def submit_live(*args, **kwargs):
    raise Unsafe("Unguarded submission forbidden; use the approved session's LiveEngine")
