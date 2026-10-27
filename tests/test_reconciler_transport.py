"""Exercise the real Argus client, including its HTTP error decorator."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import httpx
import pytest
from simple_rest_client.exceptions import (
    AuthError,
    ClientConnectionError,
    ClientError,
    ServerError,
)

from tests.test_reconciler import _config, _problem
from zabbixargus.__main__ import setup_logging
from zabbixargus.argus_client import ArgusClient
from zabbixargus.reconciler import reconcile, run_reconciliation_loop


class ArgusTransport:
    def __init__(self, boundary, failure, committed=False):
        self.boundary = boundary
        self.failure = failure
        self.committed = committed
        self.requests = []
        self.incidents = {}
        if boundary == "close":
            for pk in (1, 2):
                self.incidents[str(pk * 100)] = self.record(str(pk * 100), pk)

    @staticmethod
    def record(source_id, pk):
        return {
            "pk": pk,
            "source_incident_id": source_id,
            "description": "Synthetic problem",
            "start_time": "2023-11-14T22:13:20Z",
            "end_time": "infinity",
            "source": {"pk": 1, "name": "synthetic", "type": {"name": "test"}},
            "tags": [],
            "level": 2,
            "open": True,
        }

    def __call__(self, request):
        body = json.loads(request.content) if request.content else None
        key = (request.method, request.url.path, (body or {}).get("source_incident_id"))
        self.requests.append(key)
        target = {
            "fetch": ("GET", "/incidents/mine/", None),
            "create": ("POST", "/incidents/", "100"),
            "close": ("POST", "/incidents/1/events/", None),
        }[self.boundary]
        failure = self.failure if key == target else None
        message = (
            "Server disconnected without sending a response."
            if failure is httpx.RemoteProtocolError
            else "Synthetic failure"
        )
        if failure:
            self.failure = None
            if failure == "bad-http":
                raise httpx.RemoteProtocolError("illegal status line: b'INVALID'")
            if failure == "proxy-auth":
                raise httpx.ProxyError("407 Proxy Authentication Required")
            if failure == "bad-gzip":
                return httpx.Response(
                    200, headers={"Content-Encoding": "gzip"}, content=b"invalid gzip"
                )
            if isinstance(failure, int):
                return httpx.Response(failure, json={"detail": "Synthetic rejection"})
            if not self.committed:
                raise failure(message)

        if request.method == "GET":
            result = [i for i in self.incidents.values() if i["open"]]
        elif request.url.path == "/incidents/":
            source_id = body["source_incident_id"]
            assert source_id not in self.incidents, "Duplicate create"
            result = self.record(source_id, len(self.incidents) + 1)
            self.incidents[source_id] = result
        else:
            assert body["type"] == "END"
            pk = int(request.url.path.split("/")[2])
            incident = next(i for i in self.incidents.values() if i["pk"] == pk)
            assert incident["open"], "Duplicate close"
            incident["open"] = False
            result = dict(body, received=body["timestamp"], type={"value": "END"})
        if failure:
            raise failure(message)
        return httpx.Response(200, json=result)


@asynccontextmanager
async def transport_client(transport, url=None):
    config = _config().argus
    if url:
        config.url = url
    client = ArgusClient(config)
    await client.client.api.aclose_client()
    if not isinstance(transport, httpx.AsyncBaseTransport):
        transport = httpx.MockTransport(transport)
    async with httpx.AsyncClient(transport=transport, trust_env=False) as http:
        for resource in client.client.api._resources.values():
            resource.client = http
        yield client


@pytest.fixture(autouse=True)
def logging_setup():
    # Restore logger levels after exercising the actual CLI logging setup.
    levels = {
        logging.getLogger(name): logging.getLogger(name).level
        for name in ("simple_rest_client.decorators", "httpx")
    }
    setup_logging()
    yield
    for logger, level in levels.items():
        logger.setLevel(level)


def zabbix_for(boundary):
    problems = (
        [_problem("300", severity="1")]
        if boundary == "close"
        else [_problem("100"), _problem("200")]
    )
    return AsyncMock(
        get_problems_with_hosts=AsyncMock(return_value=problems),
        problem_exists=AsyncMock(return_value=False),
    )


@pytest.mark.parametrize("boundary", ["fetch", "create", "close"])
@pytest.mark.parametrize(
    "failure",
    [
        httpx.RemoteProtocolError,
        httpx.ReadTimeout,
        httpx.ConnectError,
    ],
)
@pytest.mark.parametrize("committed", [False, True])
async def test_connection_failure_recovers_next_pass(
    boundary, failure, committed, caplog
):
    transport = ArgusTransport(boundary, failure, committed)
    zabbix = zabbix_for(boundary)
    config = _config(webhook_enabled=False, minimum_severity=3)
    async with transport_client(transport) as argus:
        await reconcile(zabbix, argus, config)

        records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(records) == 1
        record = records[0]
        context = {
            "fetch": "fetching open incidents",
            "create": "creating incident for problem 100",
            "close": "closing incident 1 for problem 100",
        }[boundary]
        assert record.levelno == logging.WARNING
        assert context in record.getMessage()
        assert "next pass" in record.getMessage()
        assert (
            "Server disconnected without sending a response."
            if failure is httpx.RemoteProtocolError
            else "Synthetic failure"
        ) in record.getMessage()
        assert record.exc_info is None
        assert "Traceback" not in caplog.text
        assert len(transport.requests) == (1 if boundary == "fetch" else 3)
        assert len(set(transport.requests)) == len(transport.requests)
        if boundary == "create":
            assert "200" in transport.incidents
        elif boundary == "close":
            assert not transport.incidents["200"]["open"]

        caplog.clear()
        await reconcile(zabbix, argus, config)

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    if boundary == "close":
        assert all(not i["open"] for i in transport.incidents.values())
    else:
        assert set(transport.incidents) == {"100", "200"}


@pytest.mark.parametrize("boundary", ["fetch", "create", "close"])
@pytest.mark.parametrize(
    "failure, error",
    [
        (401, AuthError),
        (403, AuthError),
        (400, ClientError),
        (500, ServerError),
        (RuntimeError, RuntimeError),
        (httpx.DecodingError, ClientConnectionError),
        (httpx.TooManyRedirects, ClientConnectionError),
        (httpx.LocalProtocolError, ClientConnectionError),
        (httpx.UnsupportedProtocol, ClientConnectionError),
        ("bad-gzip", ClientConnectionError),
        ("bad-http", ClientConnectionError),
        ("proxy-auth", ClientConnectionError),
    ],
)
async def test_other_failures_keep_error_traceback(
    boundary, failure, error, caplog, monkeypatch
):
    transport = ArgusTransport(boundary, failure)
    monkeypatch.setattr(
        "zabbixargus.reconciler.asyncio.sleep",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    async with transport_client(transport) as argus:
        with pytest.raises(asyncio.CancelledError):
            await run_reconciliation_loop(
                zabbix_for(boundary),
                argus,
                _config(webhook_enabled=False, minimum_severity=3),
            )

    records = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert isinstance(records[0].exc_info[1], error)
    assert "Traceback" in caplog.text
    assert len(transport.requests) == (1 if boundary == "fetch" else 3)
    if boundary == "create":
        assert "200" in transport.incidents
    elif boundary == "close":
        assert not transport.incidents["200"]["open"]


@pytest.mark.parametrize(
    "response, proxy",
    [
        (b"", False),
        (b"INVALID\r\n\r\n", False),
        (
            b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n",
            True,
        ),
    ],
)
async def test_http_parser_distinguishes_disconnect_from_malformed_response(
    response, proxy, caplog, monkeypatch
):
    requests = []

    async def serve(reader, writer):
        requests.append(await reader.readuntil(b"\r\n\r\n"))
        writer.write(response)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    monkeypatch.setattr(
        "zabbixargus.reconciler.asyncio.sleep",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    async with await asyncio.start_server(serve, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        transport = httpx.AsyncHTTPTransport(proxy=url if proxy else None)
        if proxy:
            url = "https://argus.example.invalid"
        async with transport_client(transport, url) as argus:
            with pytest.raises(asyncio.CancelledError):
                await run_reconciliation_loop(zabbix_for("fetch"), argus, _config())

    assert len(requests) == 1
    records = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(records) == 1
    record = records[0]
    if response:
        assert record.levelno == logging.ERROR
        cause = record.exc_info[1].args[0]
        if proxy:
            assert isinstance(cause, httpx.ProxyError)
            assert "407 Proxy Authentication Required" in str(cause)
            assert requests[0].startswith(b"CONNECT argus.example.invalid:443 ")
        else:
            assert isinstance(cause, httpx.RemoteProtocolError)
            assert "illegal status line" in str(cause)
        assert "Traceback" in caplog.text
    else:
        assert record.levelno == logging.WARNING
        assert "Server disconnected without sending a response." in record.getMessage()
        assert record.exc_info is None
        assert "Traceback" not in caplog.text
