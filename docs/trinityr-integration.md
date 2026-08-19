# TrinityR integration

CopierR sits after TrinityR's permission and primary sizing boundary, not inside the detector path.

```text
TrinityR detector / strategy
          |
          v
Gamma: permission + primary sizing
          |
          v
Trinity durable intent
          |
          v
copier-client -> CopierR route fan-out
          |
          +----------------------+------------------+
          |                      |                  |
          v                      v                  v
 cTrader Open API        MetaApi MT4/MT5      future venue
          |                      |                  |
          +----------------------+------------------+
                                 |
                                 v
                    broker ACK / UNKNOWN
```

CopierR route sizing is follower-account transformation. It does not grant a strategy permission to trade and should not replace TrinityR's primary risk authority.

TrinityR remains a local/native `agent` account. Direct broker/cloud venues are internal CopierR sessions and do not use the local agent listener.

Example client setup:

```rust
use copier_client::CopierClient;
use copier_core::{HelloFrame, Platform, Side, TradeAction, TradeEvent};

let mut copier = CopierClient::connect(
    "127.0.0.1:48100",
    HelloFrame {
        account_id: "trinity-master".into(),
        platform: Platform::Trinity,
        token: std::env::var("COPIERR_TRINITY_TOKEN")?,
    },
).await?;

copier.send_event(TradeEvent {
    event_id: durable_intent_id.clone(),
    source_account_id: "trinity-master".into(),
    platform: Platform::Trinity,
    action: TradeAction::Open,
    source_order_id: durable_intent_id,
    symbol: "XAUUSD".into(),
    side: Some(Side::Buy),
    volume: approved_lots,
    remaining_volume: Some(approved_lots),
    price: None,
    stop_loss: Some(stop_price),
    take_profit: Some(target_price),
    timestamp_unix_ns: decision_timestamp_ns,
    origin_command_id: None,
}).await?;
```

Keep the Trinity intent ID stable across process restarts so CopierR deduplication remains deterministic.

`UNKNOWN` must feed back into reconciliation. Do not generate a second Trinity intent solely because an external venue acknowledgement was lost.
