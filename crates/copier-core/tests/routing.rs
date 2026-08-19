use copier_core::{CopyEngine, MirrorBinding, Platform, RouteRule, Side, SizingMode, TradeAction, TradeEvent};
use std::collections::HashMap;

fn route() -> RouteRule {
    RouteRule {
        id: "r1".into(),
        source_account_id: "master".into(),
        target_account_id: "follower".into(),
        enabled: true,
        sizing: SizingMode::Multiplier,
        size_value: 2.0,
        reverse: true,
        copy_sl_tp: true,
        min_volume: 0.01,
        max_volume: 10.0,
        volume_step: 0.01,
        allow_buy: false,
        allow_sell: false,
        allow_symbols: Vec::new(),
        deny_symbols: Vec::new(),
        symbol_map: HashMap::from([("XAUUSD".into(), "XAUUSD.a".into())]),
        symbol_prefix: String::new(),
        symbol_suffix: String::new(),
        max_event_age_ms: 0,
    }
}

fn event(action: TradeAction, volume: f64) -> TradeEvent {
    TradeEvent {
        event_id: format!("evt-{action}-{volume}"),
        source_account_id: "master".into(),
        platform: Platform::Mt5,
        action,
        source_order_id: "100".into(),
        symbol: "XAUUSD".into(),
        side: Some(Side::Buy),
        volume,
        remaining_volume: Some(0.75),
        price: None,
        stop_loss: Some(2300.0),
        take_profit: Some(2400.0),
        timestamp_unix_ns: 1,
        origin_command_id: None,
    }
}

#[test]
fn open_applies_mapping_reverse_and_multiplier() {
    let route = route();
    let engine = CopyEngine::new(vec![route.clone()]).expect("engine");
    let command = engine
        .build_command(&event(TradeAction::Open, 0.5), &route, None, 2)
        .expect("route")
        .expect("command");
    assert_eq!(command.symbol, "XAUUSD.a");
    assert_eq!(command.side, Some(Side::Sell));
    assert_eq!(command.volume, 1.0);
}

#[test]
fn reduce_scales_from_original_mirror_ratio() {
    let route = route();
    let engine = CopyEngine::new(vec![route.clone()]).expect("engine");
    let mirror = MirrorBinding {
        route_id: route.id.clone(),
        source_account_id: "master".into(),
        source_order_id: "100".into(),
        target_account_id: "follower".into(),
        target_order_id: "900".into(),
        source_open_volume: 1.0,
        source_remaining_volume: 1.0,
        target_open_volume: 2.0,
        target_remaining_volume: 2.0,
    };
    let command = engine
        .build_command(&event(TradeAction::Reduce, 0.25), &route, Some(&mirror), 2)
        .expect("route")
        .expect("command");
    assert_eq!(command.target_order_id.as_deref(), Some("900"));
    assert_eq!(command.volume, 0.5);
}
