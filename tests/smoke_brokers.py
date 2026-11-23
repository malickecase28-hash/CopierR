import base64
import hashlib
import http.server
import json
import os
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

workspace = tempfile.TemporaryDirectory(prefix="copierr-smoke-")
work = Path(workspace.name)
binary = Path(sys.argv[1]).resolve()
meta_positions, meta_orders, ctrader_positions, ctrader_orders = {}, {}, {}, {}
counts = {"meta_placements": 0, "c_trader_placements": 0, "snapshot_failures": 0}
failed = []
ambiguous = threading.Event()


class Meta(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, value, code=200):
        data = json.dumps(value).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            # The daemon may cancel the parallel orders read when positions fail.
            pass

    def do_GET(self):
        if self.path.endswith("/positions"):
            if counts["snapshot_failures"] == 0:
                counts["snapshot_failures"] += 1
                self.reply({}, 503)
                return
            self.reply(list(meta_positions.values()))
        elif self.path.endswith("/orders"):
            self.reply(list(meta_orders.values()))
        elif self.path.endswith("/specification"):
            self.reply({"minVolume": 0.01, "maxVolume": 10.0, "volumeStep": 0.01})
        else:
            self.reply({}, 404)

    def do_POST(self):
        assert self.path.endswith("/trade")
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        code = "TRADE_RETCODE_DONE"
        response = {}
        action = body["actionType"]
        if action in ("ORDER_TYPE_BUY_LIMIT", "ORDER_TYPE_BUY_STOP"):
            counts["meta_placements"] += 1
            assert body["volume"] == 0.1 and body["openPrice"] == 1.08
            meta_orders["MO-2"] = {"id": "MO-2", "type": action, "currentVolume": body["volume"], "clientId": body["clientId"]}
            response["orderId"] = "MO-2"
            code = "TRADE_RETCODE_PLACED"
        elif action == "ORDER_MODIFY":
            assert body["orderId"] in meta_orders and body["openPrice"] == 1.081
        elif action == "ORDER_CANCEL":
            del meta_orders[body["orderId"]]
        elif action == "ORDER_TYPE_BUY":
            counts["meta_placements"] += 1
            assert body["volume"] == 0.1
            meta_positions["MP-1"] = {"id": "MP-1", "type": "POSITION_TYPE_BUY", "symbol": body["symbol"],
                                      "volume": body["volume"], "openPrice": 1.09, "clientId": body["clientId"]}
            if ambiguous.is_set():
                self.reply({}, 503)
                return
            response["orderId"] = "DIFFERENT-FROM-POSITION"
        elif action == "POSITION_CLOSE_ID":
            del meta_positions[body["positionId"]]
        else:
            failed.append(f"Unrecognized MetaApi action {action}")
        self.reply({**response, "stringCode": code})


def websocket_handler(stream):
    request = stream.rfile.readline()
    headers = {}
    while True:
        line = stream.rfile.readline().decode().strip()
        if not line:
            break
        key, value = line.split(":", 1)
        headers[key.lower()] = value.strip()
    assert b"GET" in request
    key = headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    accept = base64.b64encode(hashlib.sha1(key.encode()).digest()).decode()
    stream.wfile.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                        f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode())
    stream.wfile.flush()
    while True:
        header = stream.rfile.read(2)
        if not header or header[0] & 15 == 8:
            return
        length = header[1] & 127
        if length == 126:
            length = struct.unpack("!H", stream.rfile.read(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", stream.rfile.read(8))[0]
        mask = stream.rfile.read(4)
        request = json.loads(bytes(byte ^ mask[index % 4] for index, byte in enumerate(stream.rfile.read(length))))
        payload = request["payload"]
        ctid = payload.get("ctidTraderAccountId")
        kind = request["payloadType"]
        response = None
        if kind == 2100:
            kind, response = 2101, {}
        elif kind == 2102:
            kind, response = 2103, {}
        elif kind == 2114:
            kind, response = 2115, {"symbol": [{"symbolId": 1, "symbolName": "EURUSD"}]}
        elif kind == 2116:
            kind, response = 2117, {"symbol": [{"symbolId": 1, "lotSize": 10000000, "minVolume": 100000,
                                               "maxVolume": 100000000, "stepVolume": 100000}]}
        elif kind == 2124:
            kind, response = 2125, {"position": list(ctrader_positions.values()), "order": list(ctrader_orders.values())}
        elif kind == 51:
            kind, response = 51, {}
        elif kind == 2106:
            assert payload["volume"] == 1000000
            counts["c_trader_placements"] += 1
            trade = {"symbolId": 1, "volume": payload["volume"], "tradeSide": 1, "label": payload["label"]}
            if payload["orderType"] == 1:
                position = {"positionId": 101, "positionStatus": 1, "tradeData": trade, "price": 1.09}
                ctrader_positions[101] = position
                kind, response = 2126, {"executionType": 3, "position": position}
            else:
                assert payload["orderType"] == 2 and payload["limitPrice"] == 1.08 and payload["timeInForce"] == 2
                order = {"orderId": 102, "tradeData": trade, "orderType": 2}
                ctrader_orders[102] = order
                kind, response = 2126, {"executionType": 2, "order": order}
        elif kind == 2111:
            assert payload["volume"] == 1000000
            position = ctrader_positions.pop(payload["positionId"])
            kind, response = 2126, {"executionType": 3, "position": {**position, "positionStatus": 2}}
        elif kind == 2109:
            assert payload["orderId"] in ctrader_orders and payload["limitPrice"] == 1.081
            kind, response = 2126, {"executionType": 4, "order": ctrader_orders[payload["orderId"]]}
        elif kind == 2108:
            kind, response = 2126, {"executionType": 5, "order": ctrader_orders.pop(payload["orderId"])}
        else:
            failed.append(f"Unrecognized cTrader request {kind}")
            continue
        if ctid is not None:
            response["ctidTraderAccountId"] = ctid
        msg = json.dumps({"clientMsgId": request["clientMsgId"], "payloadType": kind, "payload": response}).encode()
        hdr = bytes((0x81, len(msg))) if len(msg) < 126 else bytes((0x81, 126)) + struct.pack("!H", len(msg))
        stream.wfile.write(hdr + msg)
        stream.wfile.flush()


class BrokerTcpServer(socketserver.ThreadingTCPServer):
    daemon_threads = True


class CTrader(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            websocket_handler(self)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            failed.append(repr(exc))
            raise


def start(server):
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


meta = start(http.server.ThreadingHTTPServer(("127.0.0.1", 0), Meta))
ct = start(BrokerTcpServer(("127.0.0.1", 0), CTrader))
agent_port, ops_port = port(), port()
config = work / "simulated-brokers.toml"
config.write_text(f"""
listen = "127.0.0.1:{agent_port}"
ops_listen = "127.0.0.1:{ops_port}"
ops_token_env = "TEST_OPS"
journal_path = "{work}/simulation.journal"
checkpoint_interval_secs = 1
durability = "fsync"
pending_routes = ["copy-meta", "copy-ct"]
[follower_risk.meta]
max_open_positions = 2
max_total_lots = 0.2
[ctrader]
client_id_env = "TEST_CT_ID"
client_secret_env = "TEST_CT_SECRET"
demo_url = "ws://127.0.0.1:{ct}"
[metaapi]
auth_token_env = "TEST_META"
[[accounts]]
id = "master"
platform = "trinity"
role = "master"
token = "sim-agent"
[[accounts]]
id = "meta"
platform = "mt5"
role = "follower"
backend = "meta_api"
[accounts.metaapi]
account_id = "simulated"
api_base = "http://127.0.0.1:{meta}"
poll_interval_ms = 100
[[accounts]]
id = "ct"
platform = "ctrader"
role = "follower"
backend = "c_trader_open_api"
[accounts.ctrader]
ctid_trader_account_id = 445
access_token_env = "TEST_CT_ACCESS"
environment = "demo"
[[routes]]
id = "copy-meta"
source_account_id = "master"
target_account_id = "meta"
[[routes]]
id = "copy-ct"
source_account_id = "master"
target_account_id = "ct"
""")
env = dict(os.environ, TEST_OPS="sim-ops", TEST_CT_ID="demo", TEST_CT_SECRET="demo", TEST_CT_ACCESS="demo", TEST_META="demo")
log = (work / "simulation.log").open("w+")
proc = subprocess.Popen([binary, "daemon", "--config", config], stdout=log, stderr=log, env=env)


def request(path, body=None, auth=True):
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = "Bearer sim-ops"
    return urllib.request.urlopen(urllib.request.Request(
        f"http://127.0.0.1:{ops_port}/v1/{path}", headers=headers,
        data=None if body is None else json.dumps(body).encode(), method="GET" if body is None else "PUT",
    ), timeout=2)


def status():
    with request("status") as response:
        return json.load(response)


def wait(predicate, label, timeout=15):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline and proc.poll() is None and not failed:
        try:
            last = status()
            if predicate(last):
                print("PASS", label, flush=True)
                return last
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.05)
    raise AssertionError((label, failed, last, proc.poll()))


def emit(sock, event, order, action, price=""):
    fields = ["EVENT", "1", event, "master", "trinity", action, order, "EURUSD",
              "buy", "0.1", "", price, "", "", str(time.time_ns()), ""]
    sock.sendall(("\t".join(fields) + "\n").encode())


try:
    wait(lambda s: sum(a["snapshot_ready"] for a in s["accounts"]) == 2, "bounded reconnect after initial MetaApi snapshot failure")
    try:
        request("status", auth=False)
        raise AssertionError("unauthenticated operations request succeeded")
    except urllib.error.HTTPError as exception:
        assert exception.code == 401
    print("PASS operations bearer authentication", flush=True)

    with socket.create_connection(("127.0.0.1", agent_port), timeout=2) as sock:
        sock.sendall(b"HELLO\t1\tmaster\ttrinity\tsim-agent\n")
        assert sock.makefile("rb").readline().startswith(b"WELCOME\t1\t")
        request("pause", {"paused": True}).close()
        emit(sock, "e-market", "market", "open")
        wait(lambda s: s["queued"] == 2, "durable pause with two queued commands")
        assert counts["meta_placements"] == counts["c_trader_placements"] == 0
        request("pause", {"paused": False}).close()
        wait(lambda s: s["mirrors"] == 2 and all(a["snapshot_ready"] for a in s["accounts"] if a["account_id"] in ("ct", "meta")),
             "market fan-out, correct MetaApi position selection and cTrader broker acceptance")
        emit(sock, "e-close", "market", "close")
        wait(lambda s: s["mirrors"] == 0 and not meta_positions and not ctrader_positions, "market close uses broker position identity")
        emit(sock, "e-pending", "pending", "place_limit", "1.08")
        wait(lambda s: s["pending_mirrors"] == 2 and all(a["snapshot_ready"] for a in s["accounts"] if a["account_id"] in ("ct", "meta")),
             "limit placements and broker pending-order reconciliation")
        emit(sock, "e-amend", "pending", "amend_pending", "1.081")
        wait(lambda s: s["dispatch_to_ack"]["count"] >= 8, "pending amendment uses each broker order ticket")
        emit(sock, "e-cancel", "pending", "cancel_pending")
        wait(lambda s: s["pending_mirrors"] == 0 and not meta_orders and not ctrader_orders, "pending cancellation")
        ambiguous.set()
        emit(sock, "e-unknown", "uncertain", "open")
        state = wait(lambda s: any(item["command"]["target_account_id"] == "meta" for item in s["unknown"]), "unknown MetaApi result is held")
        assert any(alert["kind"] == "execution_unknown" for alert in state["alerts"])
        emit(sock, "e-unknown", "uncertain", "open")
        time.sleep(1.5)
        assert counts["meta_placements"] == 3
        assert status()["unknown"]
        print("PASS duplicate source event does not resubmit uncertain order", flush=True)
finally:
    proc.terminate()
    proc.wait(timeout=8)
    if failed:
        print("Mock broker failures:", failed)
    log.seek(0)
    if sys.exc_info()[0] is not None:
        print(log.read())
    log.close()
    workspace.cleanup()
