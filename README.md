# CopierR

CopierR is a Linux-first, headless Rust trade-copying service designed to run beside [TrinityR](https://github.com/malickecase28-hash/TrinityR). It normalizes a master event once, applies compiled copy rules, journals durable financial intent, and routes execution to platform-specific direct venues.

**CopierR v0.2 does not require MT4, MT5 or cTrader desktop applications to be installed on the CopierR host.**

## Architecture

```text
                         TrinityR
                            |
                    native CopierR agent
                            |
                            v
+------------------------------------------------------+
| CopierR                                              |
|                                                      |
| account registry + copy rules                        |
| deterministic command IDs                            |
| append-only journal                                  |
| mirror / position bindings                           |
| bounded venue queues                                 |
| UNKNOWN fail-closed handling                         |
+----------------------+-------------------------------+
                       |
          +------------+-------------+
          |                          |
          v                          v
  cTrader Open API              MetaApi venue
  native WebSocket              HTTPS REST v0.2
          |                          |
  cTrader backend          cloud MT4 / MT5 terminals
          |                          |
          +------------+-------------+
                       |
                    brokers
```

The local TCP listener remains only for trusted native producers such as TrinityR. MT4/MT5 and cTrader accounts are managed internally by direct venue tasks and cannot attach through that agent listener.

## Platform strategy

### cTrader

cTrader is connected natively from Rust through the official cTrader Open API. CopierR maintains at most one live hub and one demo hub and multiplexes configured accounts over those connections. The direct adapter handles application authentication, per-account authentication, symbol discovery, position reconciliation, heartbeats, order execution, partial closes, SL/TP changes and execution events.

You need a cTrader Open API application and OAuth access token for each cTrader account. You do **not** need cTrader Desktop or a cBot on the Linux host.

### MT4 / MT5

MetaTrader retail accounts do not expose a generic official broker-independent raw trading protocol comparable to cTrader Open API. CopierR therefore treats MetaTrader execution as a provider abstraction.

The first terminal-free provider is **MetaApi**. It can:

- attach to an already provisioned MetaApi account ID;
- or provision cloud MT4/MT5 access from broker login, server and password;
- submit open, modify, partial-close and close operations;
- read remote positions for master-event detection and reconciliation.

No MT4/MT5 executable, Wine prefix, MQL EA or bridge DLL is required on the CopierR host. MetaApi is an external service and requires its own account/credentials.

The provider boundary is intentionally isolated so broker FIX, broker REST, MetaTrader institutional APIs, or another MT4/MT5 cloud provider can be added without changing the copier core.

## Safety model

CopierR preserves the production-execution boundary used by TrinityR:

```text
SignalProposal
      |
      v
Gamma: permission + primary sizing
      |
      v
OMS / durable intent
      |
      v
CopierR fan-out + follower transformation
      |
      v
Venue adapter
      |
      v
ACK / UNKNOWN
      |
      v
Reconciliation
```

Commands are journaled before live dispatch. If a request has an uncertain external outcome, CopierR marks it `UNKNOWN` rather than blindly retrying and risking a duplicate financial transaction. Queued commands that were never dispatched remain safe to send when a venue reconnects.

## Current copy features

- one master to many followers;
- MT4, MT5, cTrader and TrinityR account types;
- direct cTrader Open API connectivity;
- terminal-free MetaApi MT4/MT5 connectivity;
- direct venue accounts can be master, follower or both;
- mirror, fixed-lot and multiplier sizing;
- min/max volume and volume-step normalization;
- symbol maps plus prefix/suffix mapping;
- buy/sell direction filtering;
- reverse mode;
- SL/TP propagation or suppression;
- stale-event cutoff;
- full close, modify and proportional reduce commands;
- deterministic command IDs and duplicate-event suppression;
- durable source-to-follower position bindings;
- copied-trade feedback suppression;
- append-only recovery journal.

## Linux build

Rust 1.81+ remains the workspace baseline to stay aligned with TrinityR.

```bash
git clone https://github.com/malickecase28-hash/CopierR.git
cd CopierR
cargo build --workspace --release
cargo test --workspace
```

Prepare configuration:

```bash
cp copierr.example.toml copierr.toml
```

Validate it without connecting to any venue:

```bash
cargo run -p copier-daemon -- validate --config copierr.toml
```

Run:

```bash
cargo run -p copier-daemon --release -- daemon --config copierr.toml
```

or use the built binary:

```bash
./target/release/copierr daemon --config copierr.toml
```

## Credentials

Secrets are referenced by environment-variable name and are not intended to be stored in `copierr.toml`.

Typical variables are:

```bash
export CTRADER_CLIENT_ID='...'
export CTRADER_CLIENT_SECRET='...'
export CTRADER_FOLLOWER_ACCESS_TOKEN='...'

export METAAPI_AUTH_TOKEN='...'
export MT4_FOLLOWER_PASSWORD='...'
```

See `copierr.example.toml` for complete account examples.

## TrinityR integration

`copier-client` is the native async Rust client for the local CopierR wire protocol. Configure TrinityR as an `agent` account and publish canonical `TradeEvent` values only after TrinityR's permission and primary sizing authorities have approved the intent.

See `docs/trinityr-integration.md`.

## Latency design

CopierR keeps the central fan-out path compact:

- one normalization pass at the master boundary;
- immutable compiled routing table;
- deterministic command identifiers;
- bounded in-memory venue queues;
- `TCP_NODELAY` for the TrinityR/native-agent path;
- one multiplexed cTrader connection per live/demo environment;
- persistent HTTP connection pooling for the MetaApi adapter;
- no UI, ORM or application HTTP server in the execution path;
- configurable journal durability (`none`, `flush`, `fsync`).

`durability = "none"` is for latency experiments only. Use `flush` or `fsync` according to your live durability requirements.

The cTrader path is a native persistent WebSocket. The initial MetaApi path uses synchronous REST because it maps cleanly onto CopierR's durable ACK/UNKNOWN state machine. A streaming MetaApi adapter can be added later without changing routing or journal semantics.

## Current limitations

This version deliberately fails closed where platform semantics are ambiguous:

- remote scale-ins on an existing master position are detected but not inferred into a follower command yet;
- MT5 netting semantics still need explicit reconciliation tests;
- cTrader and MetaApi credentials must be provisioned before production use;
- MetaApi is an external dependency for generic terminal-free MT4/MT5 access;
- events arriving before an open mirror binding exists are not guessed;
- no automated equity-ratio or risk-percent sizing yet;
- no built-in VPN/IP-rotation system;
- no UI.

Before live-money rollout, qualify every broker/account mode on demo accounts and add periodic full broker-state reconciliation for `UNKNOWN` resolution.

## Legacy terminal bridges

The former MQL/cBot/Rust-DLL terminal bridge implementation has been removed from the active repository. If a future deployment specifically needs self-hosted MetaTrader terminals, that should be reintroduced as a separate provider/worker service rather than coupling Windows terminal processes to the Linux copier daemon.
