"""Robinhood MCP boundary: reads by default, capability-guarded equity writes."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import time
import uuid
import re
from urllib.parse import parse_qs, urlparse
import httpx
from mcp import ClientSession
from mcp.client.auth import OAuthClientProvider
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.auth import OAuthClientInformationFull, OAuthClientMetadata, OAuthToken
from mcp.types import PaginatedRequestParams
from .models import UTC, iso, dt, Unsafe, PreSubmissionBlocked
from .diagnostics import event as diagnostic_event, exception_record, hooks, redact
from mcp.shared.exceptions import McpError

ENDPOINT = "https://agent.robinhood.com/mcp/trading"
READ_TOOLS = frozenset({"get_accounts", "get_portfolio", "get_equity_positions", "get_equity_orders", "get_equity_quotes", "get_equity_tradability", "get_equity_historicals", "review_equity_order"})


def failure_types(exc):
    """Useful transport diagnostics without response bodies, tokens or URLs."""
    if isinstance(exc, BaseExceptionGroup):
        return sorted({kind for child in exc.exceptions for kind in failure_types(child)})
    return [type(exc).__name__]


def failure_details(exc):
    if isinstance(exc, BaseExceptionGroup):
        return list(dict.fromkeys(d for child in exc.exceptions for d in failure_details(child)))
    if isinstance(exc, Unsafe):
        return [str(exc)]  # Locally constructed, sanitized broker errors.
    if isinstance(exc, httpx.HTTPStatusError):
        return [f'HTTPStatusError({exc.response.status_code})']
    if isinstance(exc, McpError):
        return [f'McpError(code={exc.error.code}): {redact(exc.error.message)}']
    return [type(exc).__name__]


class BrokerToolError(Unsafe):
    """A completed MCP error response: connection is still usable; never retry writes."""
    def __init__(self, tool, result):
        texts = [c.text for c in result.content if getattr(c,'type',None)=='text']
        raw = json.dumps(result.structuredContent) if result.structuredContent else ' '.join(texts)
        raw = re.sub(r'(?i)(bearer\s+|access_token[\"\s:=]+|refresh_token[\"\s:=]+)[^\s\",}]+',r'\1[REDACTED]',raw)
        raw = re.sub(r'\b\d{8,}\b','[REDACTED]',raw)
        self.tool = tool
        self.retry_after = 60 if any(s in raw.lower() for s in ('rate limit','rate_limit','throttl','429')) else 5
        delay=re.search(r'available in (\d+(?:\.\d+)?) seconds',raw,re.IGNORECASE)
        if delay:
            self.retry_after=max(1,float(delay.group(1))+.5)
        super().__init__(f'Robinhood {tool} tool error: {raw[:500]}')


async def discover_tools(session):
    """Warm the SDK schema cache before bounded account/trading calls."""
    found, cursors, cursor = {}, set(), None
    for _ in range(100):
        result = await session.list_tools(params=PaginatedRequestParams(cursor=cursor) if cursor else None)
        for tool in result.tools:
            if tool.name in found:
                raise Unsafe("Duplicate tool in Robinhood discovery")
            found[tool.name] = tool
        cursor = result.nextCursor
        if not cursor:
            missing = READ_TOOLS - found.keys()
            if missing:
                raise Unsafe("Required Robinhood tools missing: " + ", ".join(sorted(missing)))
            return found
        if cursor in cursors:
            raise Unsafe("Repeated Robinhood tool-discovery cursor")
        cursors.add(cursor)
    raise Unsafe("Robinhood tool discovery exceeded pagination limit")


class TokenFile:
    def __init__(self, path):
        self.path = Path(path)

    def read(self):
        if not self.path.exists():
            return {}
        if self.path.stat().st_mode & 0o077:
            raise Unsafe("OAuth file permissions must be 0600")
        return json.loads(self.path.read_text())

    def write(self, key, value):
        data = self.read()
        data[key] = value.model_dump(mode="json")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temp = self.path.with_suffix(".tmp")
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.chmod(temp, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, self.path)

    async def get_tokens(self):
        value = self.read().get("tokens")
        return OAuthToken.model_validate(value) if value else None

    async def set_tokens(self, value):
        self.write("tokens", value)

    async def get_client_info(self):
        value = self.read().get("client")
        return OAuthClientInformationFull.model_validate(value) if value else None

    async def set_client_info(self, value):
        self.write("client", value)


@asynccontextmanager
async def connect(token_path, interactive=False):
    async def redirect(url):
        if not interactive:
            raise Unsafe("Authentication required; run the auth command interactively")
        print("Open this Robinhood authorization URL in your browser:\n" + url, flush=True)

    async def callback():
        if not interactive:
            raise Unsafe("Interactive authentication is disabled in the service")
        # Bound before URL is printed by provider? A short browser visit before
        # callback starts can be retried; SDK validates OAuth state itself.
        from http.server import HTTPServer, BaseHTTPRequestHandler
        result = {}

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                query = urlparse(self.path)
                if query.path != "/callback":
                    self.send_error(404)
                    return
                result.update(parse_qs(query.query))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"Authorization received. Return to the terminal.")

            def log_message(self, *args):
                pass  # Never log authorization codes.

        with HTTPServer(("127.0.0.1", 8765), Handler) as server:
            server.timeout = 180
            await asyncio.to_thread(server.handle_request)
        if "code" not in result:
            raise Unsafe("Authorization did not return a code")
        return result["code"][0], result.get("state", [None])[0]

    from .host_health import active_config
    from .configuration import DEFAULTS
    production=active_config(Path(token_path).parent)
    transport=(production or DEFAULTS)['connectivity']
    storage = TokenFile(token_path)
    if not interactive and not await storage.get_tokens():
        raise Unsafe("No standalone Robinhood credentials. Run: python -m trader auth")
    auth = OAuthClientProvider(
        server_url=ENDPOINT,
        client_metadata=OAuthClientMetadata(client_name="Intraday Research", redirect_uris=["http://127.0.0.1:8765/callback"], grant_types=["authorization_code", "refresh_token"], response_types=["code"]),
        storage=storage, redirect_handler=redirect, callback_handler=callback)
    # SSE is a long-lived transport; RPC deadlines belong to ClientSession.
    # Discovery can take ~7s and must not consume an account call's 8s budget.
    stage, started = "initialize", time.monotonic()
    connection_id=str(uuid.uuid4())
    runtime=Path(token_path).parent
    transport_failure_context={};broker=None
    try:
        # HTTP/1.1 SSE responses close before EOF and were forcing a new TCP/TLS
        # connection for every RPC. HTTP/2 can close each response stream while
        # retaining the connection. Keep idle connections past the 30s idle poll.
        async with httpx.AsyncClient(auth=auth, http2=True,
                limits=httpx.Limits(max_connections=10,max_keepalive_connections=5,keepalive_expiry=transport['keepalive_expiry_seconds']),
                timeout=httpx.Timeout(transport['request_timeout_seconds'],connect=transport['connect_timeout_seconds'],read=transport['stream_read_timeout_seconds']), headers={"x-timezone-id": "America/New_York"},event_hooks=hooks(runtime,connection_id,level=(production or DEFAULTS)['logging']['level'],failure_context=transport_failure_context)) as http:
            # This endpoint rejects session DELETE with 400. The SDK option
            # still cancels/joins transport tasks and closes all local streams.
            async with streamable_http_client(ENDPOINT, http_client=http, terminate_on_close=False) as (read, write, _):
                async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=240 if interactive else transport['initialize_timeout_seconds'])) as session:
                    initialized = await session.initialize()
                    stage = "list_tools"
                    catalog = await discover_tools(session)
                    broker = Robinhood(session,timeout=transport['request_timeout_seconds'])
                    broker.account_cache_seconds=transport['account_cache_seconds']
                    broker.diagnostic_runtime = runtime
                    broker.catalog = catalog
                    broker.connection_info = dict(protocol_version=initialized.protocolVersion, initialized=True,
                        tools_discovered=len(catalog), setup_seconds=round(time.monotonic()-started, 3), connection_id=connection_id)
                    stage = "connected"
                    yield broker
                    stage = "close"
    except Exception as exc:
        exc.recovery_context=dict(getattr(broker,'failure_context',{}) or transport_failure_context,
            connection_id=connection_id,connection_age_seconds=round(time.monotonic()-started,3),
            successful_calls=getattr(broker,'successful_calls',0))
        diagnostic_event(runtime,'CONNECTION_ERROR',connection_id=connection_id,stage=stage,lifetime_seconds=round(time.monotonic()-started,3),exception=exception_record(exc))
        if stage in ("initialize", "list_tools", "close"):
            raise Unsafe(f"Robinhood MCP {stage} failed: {', '.join(failure_types(exc))}") from exc
        raise
    finally:
        diagnostic_event(runtime,'CONNECTION_CLOSED',connection_id=connection_id,stage=stage,lifetime_seconds=round(time.monotonic()-started,3))


class Robinhood:
    def __init__(self, client, timeout=8):
        self.client, self.timeout = client, timeout
        self.live_guard = None
        self.cancel_guard = None
        self.catalog = None
        self.connection_info = {}
        self.account_cache = {}
        self.account_cache_seconds = 8
        self.observation_lock = asyncio.Lock()
        self.cycle_error = None
        self.last_failed_tool = None
        self.call_counts = {}
        self.diagnostic_runtime = None
        self.connected_monotonic = time.monotonic()
        self.successful_calls = 0
        self.active_tools = set()
        self.failure_context = {}

    def bind_live_guard(self, guard, cancel_guard=None):
        self.live_guard = guard
        self.cancel_guard = cancel_guard

    async def call(self, name, args):
        if name not in READ_TOOLS:
            raise Unsafe(f"Network write blocked: {name}")
        return await self._request(name, args)

    async def _request(self, name, args):
        if self.catalog is not None and name not in self.catalog:
            raise PreSubmissionBlocked("Requested operation was not advertised by Robinhood")
        if name not in READ_TOOLS:
            guard = self.live_guard if name=="place_equity_order" else self.cancel_guard if name=="cancel_equity_order" else None
            if guard is None:
                raise Unsafe("Network mutation has no approved capability")
            try:
                guard(args)
                # Standalone real transport requires independent host evidence.
                # Risk-reducing sells/cancellations retain their existing guards.
                if name == 'place_equity_order' and args.get('side') == 'buy' and self.diagnostic_runtime is not None:
                    from .host_health import require_host
                    require_host(self.diagnostic_runtime)
            except Unsafe as exc:
                raise PreSubmissionBlocked(str(exc)) from exc
        self.call_counts[name] = self.call_counts.get(name,0)+1
        started=time.monotonic()
        self.active_tools.add(name)
        if self.diagnostic_runtime is not None:
            # The cycle heartbeat is otherwise stamped once, before review and
            # reconciliation. Refresh it around each RPC so in-flight broker
            # work is not mistaken for a dead worker.
            from .service import heartbeat
            heartbeat(self.diagnostic_runtime, 'POLLING')
        try:
            result = await self.client.call_tool(name, args, read_timeout_seconds=timedelta(seconds=self.timeout))
        except Exception as exc:
            self.cycle_error = exc
            self.last_failed_tool = name
            self.failure_context=dict(tool=name,connection_id=self.connection_info.get('connection_id'),
                connection_age_seconds=time.monotonic()-self.connected_monotonic,successful_calls=self.successful_calls)
            self.account_cache.clear()
            if self.diagnostic_runtime:
                diagnostic_event(self.diagnostic_runtime,'RPC_ERROR',connection_id=self.connection_info.get('connection_id'),tool=name,elapsed_seconds=round(time.monotonic()-started,3),timeout_seconds=self.timeout,exception=exception_record(exc))
            raise
        finally:
            self.active_tools.discard(name)
        if self.diagnostic_runtime:
            diagnostic_event(self.diagnostic_runtime,'RPC_RESULT',connection_id=self.connection_info.get('connection_id'),tool=name,elapsed_seconds=round(time.monotonic()-started,3),is_error=result.isError)
        if result.isError:
            self.cycle_error = BrokerToolError(name,result)
            self.last_failed_tool = name
            self.account_cache.clear()
            raise self.cycle_error
        payload = result.structuredContent
        if payload is None:
            blocks = [c.text for c in result.content if getattr(c, "type", None) == "text"]
            if len(blocks) != 1:
                raise Unsafe("Unknown MCP response")
            payload = json.loads(blocks[0])
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise Unsafe("Missing MCP data")
        self.successful_calls += 1
        return payload["data"]

    async def pages(self, name, key, account):
        args, rows, seen = {"account_number": account}, [], set()
        for _ in range(100):
            data = await self.call(name, args)
            rows.extend(data[key] or [])
            if not data.get("next"):
                return rows
            cursor = parse_qs(urlparse(data["next"]).query).get("cursor", [None])[0]
            if not cursor or cursor in seen:
                raise Unsafe("Invalid pagination")
            seen.add(cursor)
            args["cursor"] = cursor
        raise Unsafe("Pagination limit: cannot reconcile complete state")

    async def snapshot(self, account):
        # Only explicit, pinned account is read; no default-account selection.
        started = datetime.now(UTC)
        started_monotonic = time.monotonic()
        accounts, portfolio, positions, orders = await asyncio.gather(
            self.call("get_accounts", {}), self.call("get_portfolio", {"account_number": account}),
            self.pages("get_equity_positions", "positions", account),
            self.pages("get_equity_orders", "orders", account))
        matches = [a for a in accounts["accounts"] if a and a["account_number"] == account]
        if len(matches) != 1:
            raise Unsafe("Pinned Agentic account missing")
        self.account_cache[account] = (started_monotonic, iso(started), matches[0], portfolio)
        return dict(observed_at=iso(started), account=matches[0], portfolio=portfolio, positions=positions, orders=orders)

    async def observe(self, account):
        """Poll holdings/orders every cycle; refresh core data after eight seconds.

        observed_at is the OLDEST input, not the time the cached data is reused.
        All order preflights continue to use snapshot(), which bypasses the cache.
        """
        async with self.observation_lock:
            cached = self.account_cache.get(account)
            # macOS monotonic time can pause during sleep. Wall time must also
            # qualify cached account evidence after wake or a clock correction.
            if (cached is None or not 0 <= time.monotonic()-cached[0] < self.account_cache_seconds
                    or not 0 <= (datetime.now(UTC)-dt(cached[1])).total_seconds() < self.account_cache_seconds):
                return await self.snapshot(account)
            _, observed, account_data, portfolio = cached
            positions, orders = await asyncio.gather(
                self.pages('get_equity_positions','positions',account),
                self.pages('get_equity_orders','orders',account))
            return dict(observed_at=observed,account=account_data,portfolio=portfolio,positions=positions,orders=orders)

    async def quotes(self, symbols):
        data = await self.call("get_equity_quotes", {"symbols": list(symbols)})
        return {r["quote"]["symbol"]: r["quote"] for r in data["results"] or [] if r and r.get("quote")}

    async def tradability(self, account, symbols):
        data = await self.call("get_equity_tradability", {"account_number": account, "symbols": list(symbols)})
        return {r["symbol"]: r for r in data["results"] or []}

    async def bars(self, symbols, session, now):
        symbols = list(symbols)
        interval = timedelta(minutes=5)
        # Robinhood aligns five-minute range endpoints to interval boundaries.
        # Before the first completed bar, both endpoints become the open and
        # upstream rejects the zero-width range. No bars yet is expected, not
        # a connection failure. Never request a future or incomplete interval.
        completed = (min(now, session.close) - session.open) // interval
        if completed < 1:
            return {symbol: [] for symbol in symbols}
        end = session.open + completed * interval
        data = await self.call("get_equity_historicals", dict(symbols=symbols, start_time=iso(session.open), end_time=iso(end), interval="5minute", bounds="regular", adjustment_type="none"))
        if any(r.get("interval") != "5minute" for r in data["results"] or []):
            raise Unsafe("Unexpected bar interval")
        return {r["symbol"]: r["bars"] or [] for r in data["results"] or []}

    async def review(self, account, symbol, dollars):
        return await self.call("review_equity_order", dict(account_number=account, symbol=symbol, side="buy", dollar_amount=str(dollars), type="market", market_hours="regular_hours", time_in_force="gfd"))

    async def review_order(self, args):
        return await self.call("review_equity_order", {k:v for k,v in args.items() if k != "ref_id"})

    async def order(self, account, order_id):
        result = await self.call("get_equity_orders", {"account_number":account, "order_id":order_id})
        orders = result.get("orders") or []
        if len(orders) != 1 or orders[0].get("id") != order_id:
            raise Unsafe("Broker order identity unavailable")
        return orders[0]

    async def submit(self, args):
        if self.live_guard is None:
            raise Unsafe("LIVE submission has no authorization capability")
        # Never retry this request automatically, including authentication or transport errors.
        return await self._request("place_equity_order", args)

    async def cancel_owned(self,args):
        if self.cancel_guard is None:
            raise Unsafe("No owned-order cancellation capability")
        return await self._request("cancel_equity_order",args)
