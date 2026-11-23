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

#[test]
fn rejects_empty_symbol_map_entries() {
    let mut invalid = route();
    invalid.symbol_map.insert("".into(), "EURUSD".into());
    assert!(CopyEngine::new(vec![invalid]).is_err());
}

fn plain_route() -> RouteRule {
    RouteRule { reverse: false, sizing: SizingMode::Mirror, ..route() }
}

#[test]
fn reverse_swaps_stop_loss_and_take_profit() {
    let route = route(); // reverse = true, copy_sl_tp = true
    let engine = CopyEngine::new(vec![route.clone()]).expect("engine");
    let command = engine
        .build_command(&event(TradeAction::Open, 0.5), &route, None, 2)
        .expect("route")
        .expect("command");
    // the source buy has its stop at 2300 and its target at 2400; the reversed sell has them the other way round
    assert_eq!(command.side, Some(Side::Sell));
    assert_eq!(command.stop_loss, Some(2400.0));
    assert_eq!(command.take_profit, Some(2300.0));

    let straight = plain_route();
    let command = CopyEngine::new(vec![straight.clone()]).unwrap()
        .build_command(&event(TradeAction::Open, 0.5), &straight, None, 2).unwrap().unwrap();
    assert_eq!((command.stop_loss, command.take_profit), (Some(2300.0), Some(2400.0)));
}

#[test]
fn open_below_route_minimum_is_refused_not_raised() {
    let route = RouteRule { size_value: 0.1, ..route() }; // multiplier 0.1, minimum 0.01
    let engine = CopyEngine::new(vec![route.clone()]).expect("engine");
    // 0.05 x 0.1 = 0.005: below the 0.01 minimum, so nothing is opened (it used to open 0.01, twice the intended size)
    assert!(engine.build_command(&event(TradeAction::Open, 0.05), &route, None, 2).unwrap().is_none());
    // 0.1 x 0.1 = 0.01: exactly the minimum, opened
    let command = engine.build_command(&event(TradeAction::Open, 0.1), &route, None, 2).unwrap().unwrap();
    assert_eq!(command.volume, 0.01);
    // above the maximum the route's cap still applies
    let capped = RouteRule { max_volume: 0.5, ..plain_route() };
    let command = CopyEngine::new(vec![capped.clone()]).unwrap()
        .build_command(&event(TradeAction::Open, 2.0), &capped, None, 2).unwrap().unwrap();
    assert_eq!(command.volume, 0.5);
}

#[test]
fn switched_off_or_narrowed_route_still_follows_its_copies() {
    let mirror = MirrorBinding {
        route_id: "r1".into(),
        source_account_id: "master".into(),
        source_order_id: "100".into(),
        target_account_id: "follower".into(),
        target_order_id: "900".into(),
        source_open_volume: 1.0,
        source_remaining_volume: 1.0,
        target_open_volume: 1.0,
        target_remaining_volume: 1.0,
    };
    for route in [
        RouteRule { enabled: false, ..plain_route() },
        RouteRule { deny_symbols: vec!["XAUUSD".into()], ..plain_route() },
        RouteRule { allow_sell: true, ..plain_route() }, // sell-only: the source buy is no longer allowed as new exposure
    ] {
        let engine = CopyEngine::new(vec![route.clone()]).expect("engine");
        assert!(engine.build_command(&event(TradeAction::Open, 1.0), &route, None, 2).unwrap().is_none(), "no new exposure");
        let close = engine.build_command(&event(TradeAction::Close, 1.0), &route, Some(&mirror), 2).unwrap().unwrap();
        assert_eq!(close.target_order_id.as_deref(), Some("900"));
        let reduce = engine.build_command(&event(TradeAction::Reduce, 0.5), &route, Some(&mirror), 2).unwrap().unwrap();
        assert_eq!(reduce.volume, 0.5);
        let modify = engine.build_command(&event(TradeAction::Modify, 1.0), &route, Some(&mirror), 2).unwrap().unwrap();
        assert_eq!(modify.stop_loss, Some(2300.0));
    }
}
