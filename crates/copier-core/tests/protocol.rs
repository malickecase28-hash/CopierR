use copier_core::{
    encode_agent_frame, parse_agent_line, AgentFrame, Platform, Side, TradeAction, TradeEvent,
};
use serde_json::json;

#[test]
fn event_wire_roundtrip() {
    let event = TradeEvent {
        event_id: "evt-1".into(),
        source_account_id: "master".into(),
        platform: Platform::Mt5,
        action: TradeAction::Open,
        source_order_id: "42".into(),
        symbol: "EURUSD".into(),
        side: Some(Side::Buy),
        volume: 0.25,
        remaining_volume: Some(0.25),
        price: Some(1.1),
        stop_loss: Some(1.09),
        take_profit: Some(1.12),
        timestamp_unix_ns: 123,
        origin_command_id: None,
    };
    let encoded = encode_agent_frame(&AgentFrame::Event(event.clone()));
    let decoded = parse_agent_line(&encoded).expect("wire frame parses");
    assert_eq!(decoded, AgentFrame::Event(event));
}

#[test]
fn ctrader_accepts_documented_config_name_and_legacy_alias() {
    assert_eq!(
        serde_json::from_value::<Platform>(json!("ctrader")).unwrap(),
        Platform::CTrader
    );
    assert_eq!(
        serde_json::from_value::<Platform>(json!("c_trader")).unwrap(),
        Platform::CTrader
    );
    assert_eq!(serde_json::to_value(Platform::CTrader).unwrap(), json!("ctrader"));
}
