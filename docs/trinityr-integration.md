# TrinityR integration

CopierR should sit after TrinityR's permission and sizing boundary, not inside
the detector path.

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
          v
MT4 / MT5 / cTrader terminal agents
          |
          v
broker acknowledgement / UNKNOWN
```

CopierR's route sizing is follower-account transformation. It does not grant a
strategy permission to trade and should not replace TrinityR's primary risk
authority.

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

For production, keep the Trinity intent ID stable across process restarts so
CopierR deduplication remains deterministic.

`UNKNOWN` must feed back into reconciliation. Do not generate a second Trinity
intent solely because the first terminal ACK was lost.
