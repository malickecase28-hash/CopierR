# CopierR

CopierR is a low-latency Rust trade-copying daemon designed to run beside
[TrinityR](https://github.com/malickecase28-hash/TrinityR). It normalizes a
master event once, fans it out through compiled copy rules, and dispatches
platform-specific execution commands to MT4, MT5 and cTrader terminal agents.

The initial implementation is intentionally headless. There is no UI in the
hot path.

## Architecture

```text
TrinityR / master terminal
          |
          | normalized EVENT
          v
+-------------------------------+
| CopierR                       |
|                               |
| auth + account registry       |
| compiled route rules          |
| deterministic command IDs     |
| mirror/ticket book            |
| append-only journal           |
| UNKNOWN fail-closed handling  |
+---------------+---------------+
                |
        localhost TCP, TCP_NODELAY
        +-------+-------+
        |               |
        v               v
  MT4 / MT5 DLL      cTrader cBot
        |               |
        v               v
    terminal         terminal
        |               |
        +----- broker ---+
```

MT4 and MT5 use the small Rust `copierr_bridge` DLL only as a nonblocking TCP
transport. Trading logic remains in the EA and the central Rust daemon. cTrader
uses `TcpClient` directly from the cBot.

## Safety model

CopierR follows TrinityR's production-execution direction: external outcomes
that are uncertain are not blindly retried. A command is journaled as queued,
then journaled as dispatched before it enters a live terminal session. If that
session disappears before a terminal ACK, the command becomes `UNKNOWN` and is
not replayed automatically.

Queued commands that were never dispatched are safe to send when the target
account reconnects.

The journal also persists master-event deduplication and master-to-follower
order bindings.

## Current copy features

- one master to many followers;
- MT4, MT5 and cTrader account types;
- TrinityR as a native Rust master source;
- mirror, fixed-lot and multiplier sizing;
- min/max volume and volume-step normalization;
- symbol maps plus prefix/suffix mapping;
- buy/sell direction filtering;
- reverse mode;
- SL/TP propagation or suppression;
- stale-event cutoff;
- full close, modify and proportional reduce commands;
- deterministic command IDs and duplicate-event suppression;
- durable ticket/position mirror bindings;
- optional terminal process supervision;
- fixed per-account egress profiles.

## Network / regional egress

CopierR does not rotate IPs or attempt to bypass broker controls. The terminal
supervisor can attach an account to a fixed authorized egress path:

- `direct`;
- `proxy_env` for software that honors `ALL_PROXY` / `HTTP_PROXY` /
  `HTTPS_PROXY`;
- `network_namespace` on Linux/Wine, where the namespace and VPN/tunnel are
  provisioned outside CopierR.

This keeps account network identities isolated without putting VPN setup or
rotation logic into the trade engine.

## Build

Rust 1.81+ is the workspace baseline.

```bash
cargo build --workspace --release
cargo test --workspace
```

Run the daemon:

```bash
cp copierr.example.toml copierr.toml
cargo run -p copier-daemon --release -- daemon --config copierr.toml
```

Validate config without starting listeners or terminals:

```bash
cargo run -p copier-daemon -- validate --config copierr.toml
```

## MT4 / MT5 bridge DLL

Build both Windows architectures because MT4 is normally 32-bit while MT5 is
normally 64-bit:

```powershell
rustup target add i686-pc-windows-msvc x86_64-pc-windows-msvc
cargo build -p copier-bridge-ffi --release --target i686-pc-windows-msvc
cargo build -p copier-bridge-ffi --release --target x86_64-pc-windows-msvc
```

Copy the 32-bit DLL into the MT4 `MQL4/Libraries` directory and the 64-bit DLL
into the MT5 `MQL5/Libraries` directory. Enable DLL imports for the CopierR EA.
The EA source lives under `bridges/mt4` and `bridges/mt5`.

## TrinityR integration

`copier-client` is a small async Rust client that shares CopierR's canonical
wire contract. Add it to TrinityR as a path or Git dependency, connect with an
account configured as `platform = "trinity"`, and submit `TradeEvent` values
after TrinityR's permission/sizing authority has approved the intent.

See `docs/trinityr-integration.md` for the intended authority boundary.

## Low-latency choices

- one normalization pass at the master boundary;
- immutable compiled routing table;
- `TCP_NODELAY` on every local session;
- bounded per-terminal queues;
- compact tab-delimited frames rather than general-purpose broker JSON;
- a Rust `cdylib` transport for MetaTrader instead of file polling;
- append-only journal with configurable `none`, `flush`, or `fsync` durability;
- no UI, database ORM, HTTP framework or cloud hop in the initial hot path.

`durability = "none"` is suitable only for latency experiments. Use `flush` or
`fsync` for live financial execution depending on your durability requirements.

## Initial limitations

This first cut is deliberately narrow:

- market-position copying is the primary path;
- MT4 broker-specific partial-close ticket replacement needs live broker
  qualification;
- MT5 netting-account scale-in semantics need dedicated reconciliation work;
- events that arrive before an open mirror binding exists are logged and
  skipped for that route rather than guessed;
- no automated account-equity sizing yet;
- no built-in VPN provisioning;
- no UI.

Before live rollout, qualify each broker/account mode in demo accounts and add
periodic broker-state reconciliation. The daemon is designed so reconciliation
can resolve `UNKNOWN` outcomes without weakening idempotency.

## Reference research

The design was informed by public trade-copier patterns including Cascada's
small terminal bridges plus Rust core and ejtraderCP's publisher/subscriber
model. CopierR is an independent implementation and does not vendor those
projects' source.
