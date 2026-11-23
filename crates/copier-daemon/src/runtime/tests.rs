use super::*;
use std::path::PathBuf;

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "copierr-test-{}-{}",
            std::process::id(),
            unix_time_ns()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn journal(&self) -> PathBuf {
        self.0.join("journal.jsonl")
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(extra: &str) -> Arc<DaemonConfig> {
    Arc::new(
        toml::from_str(&format!(
            r#"
durability = "fsync"
pending_routes = ["copy"]
{extra}
[[accounts]]
id = "master"
platform = "trinity"
role = "master"
token = "test"
[[accounts]]
id = "follower"
platform = "mt5"
role = "follower"
token = "test"
[[routes]]
id = "copy"
source_account_id = "master"
target_account_id = "follower"
"#
        ))
        .unwrap(),
    )
}

async fn open(dir: &TestDir, extra: &str) -> (Arc<Journal>, AppState) {
    let config = config(extra);
    config.validate().unwrap();
    let (journal, replay) = Journal::open(dir.journal(), config.durability)
        .await
        .unwrap();
    let journal = Arc::new(journal);
    (
        journal.clone(),
        AppState::new(config, journal, replay).unwrap(),
    )
}

async fn connect(app: &AppState) -> (u64, mpsc::Receiver<ServerFrame>) {
    let (tx, rx) = mpsc::channel(1);
    (
        app.register_session("follower".to_owned(), tx)
            .await
            .unwrap(),
        rx,
    )
}

fn event(id: &str, order: &str, action: TradeAction) -> TradeEvent {
    TradeEvent {
        event_id: id.to_owned(),
        source_account_id: "master".to_owned(),
        platform: copier_core::Platform::Trinity,
        action,
        source_order_id: order.to_owned(),
        symbol: "EURUSD".to_owned(),
        side: Some(copier_core::Side::Buy),
        volume: 0.1,
        remaining_volume: None,
        price: action.is_pending().then_some(1.08),
        stop_loss: None,
        take_profit: None,
        timestamp_unix_ns: unix_time_ns(),
        origin_command_id: None,
    }
}

fn command(rx: &mut mpsc::Receiver<ServerFrame>) -> ExecutionCommand {
    match rx.try_recv().unwrap() {
        ServerFrame::Command(command) => *command,
        _ => panic!("expected trade command"),
    }
}

fn ack(command: &ExecutionCommand, external_id: Option<&str>) -> ExecutionAck {
    ExecutionAck {
        command_id: command.command_id.clone(),
        account_id: "follower".to_owned(),
        status: AckStatus::Filled,
        external_id: external_id.map(str::to_owned),
        timestamp_unix_ns: unix_time_ns(),
        message: None,
    }
}

#[tokio::test]
async fn concurrent_duplicates_and_deferred_close_dispatch_once() {
    let dir = TestDir::new();
    let (_, app) = open(&dir, "").await;
    let (_, mut rx) = connect(&app).await;
    let original = event("open", "order", TradeAction::Open);
    let (a, b) = tokio::join!(
        app.handle_event(original.clone()),
        app.handle_event(original)
    );
    a.unwrap();
    b.unwrap();
    app.pump().await.unwrap();
    let open = command(&mut rx);
    app.handle_event(event("close", "order", TradeAction::Close))
        .await
        .unwrap();
    app.pump().await.unwrap();
    assert!(rx.try_recv().is_err());
    app.handle_frame("follower", AgentFrame::Ack(ack(&open, Some("broker-7"))))
        .await
        .unwrap();
    app.pump().await.unwrap();
    let close = command(&mut rx);
    assert_eq!(close.action, TradeAction::Close);
    assert_eq!(close.target_order_id.as_deref(), Some("broker-7"));
    app.pump().await.unwrap();
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn restart_keeps_paused_state_and_never_resends_uncertain_dispatch() {
    let dir = TestDir::new();
    let (journal, app) = open(&dir, "").await;
    let (_, mut rx) = connect(&app).await;
    app.handle_event(event("open", "order", TradeAction::Open))
        .await
        .unwrap();
    app.pump().await.unwrap();
    let sent = command(&mut rx);
    assert!(Journal::open(dir.journal(), app.config.durability)
        .await
        .is_err());

    app.set_paused(true).await.unwrap();
    journal
        .compact(&app.runtime.lock().await.ledger)
        .await
        .unwrap();
    drop(app);
    drop(journal);

    let (_, recovered) = open(&dir, "").await;
    let (_, mut rx) = connect(&recovered).await;
    assert_eq!(recovered.status().await["paused"], true);
    assert_eq!(
        recovered.status().await["unknown"][0]["command"]["command_id"],
        sent.command_id
    );
    recovered
        .handle_event(event("open", "order", TradeAction::Open))
        .await
        .unwrap();
    recovered.set_paused(false).await.unwrap();
    recovered.pump().await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(recovered.status().await["queued"], 0);
}

#[tokio::test]
async fn restart_replays_legacy_jsonl_and_reports_disconnect() {
    let dir = TestDir::new();
    std::fs::write(
        dir.journal(),
        "{\"kind\":\"event_planned\",\"event_id\":\"legacy\",\"commands\":[]}\n",
    )
    .unwrap();
    let (_, app) = open(&dir, "").await;
    let (session, mut rx) = connect(&app).await;
    app.handle_event(event("legacy", "old", TradeAction::Open))
        .await
        .unwrap();
    app.pump().await.unwrap();
    assert!(rx.try_recv().is_err());

    app.handle_event(event("new", "new", TradeAction::Open))
        .await
        .unwrap();
    app.pump().await.unwrap();
    let dispatched = command(&mut rx);
    app.unregister_session("follower", session).await.unwrap();
    let status = app.status().await;
    assert_eq!(
        status["unknown"][0]["command"]["command_id"],
        dispatched.command_id
    );
    assert_eq!(status["accounts"][1]["connected"], false);
}

#[tokio::test]
async fn checkpointed_retention_floor_survives_disabling_expiry() {
    let dir = TestDir::new();
    let (journal, app) = open(&dir, "event_retention_secs = 60").await;
    let original = event("old-event", "old-order", TradeAction::Open);
    app.handle_event(original.clone()).await.unwrap();
    let mut runtime = app.runtime.lock().await;
    runtime.ledger.prune_events_before(original.timestamp_unix_ns + 1);
    assert!(runtime.ledger.seen_events.is_empty());
    journal.compact(&runtime.ledger).await.unwrap();
    drop(runtime);
    drop(app);
    drop(journal);

    let (_, app) = open(&dir, "").await;
    assert!(app.handle_event(original).await.is_err());
}

#[tokio::test]
async fn disconnected_follower_does_not_open_a_trade_already_closed_at_source() {
    let dir = TestDir::new();
    let (_, app) = open(&dir, "").await;
    app.handle_event(event("open", "order", TradeAction::Open))
        .await
        .unwrap();
    app.handle_event(event("close", "order", TradeAction::Close))
        .await
        .unwrap();
    let (_, mut rx) = connect(&app).await;
    app.pump().await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(app.status().await["queued"], 0);
}

#[tokio::test]
async fn caps_require_snapshot_and_include_existing_exposure() {
    let dir = TestDir::new();
    let (_, app) = open(&dir, "[follower_risk.follower]\nmax_total_lots = 0.15").await;
    let (_, mut rx) = connect(&app).await;
    app.handle_event(event("open", "new", TradeAction::Open))
        .await
        .unwrap();
    app.pump().await.unwrap();
    assert!(rx.try_recv().is_err());
    app.observe_positions(
        "follower",
        vec![BrokerPosition {
            id: "manual".to_owned(),
            lots: 0.1,
            copier_managed: false,
            pending: false,
        }],
    )
    .await
    .unwrap();
    app.pump().await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(
        app.status().await["recent_failures"][0]["message"],
        "follower gross lots limit reached"
    );
}

#[tokio::test]
async fn pending_limit_can_be_amended_and_cancelled_using_its_broker_order_id() {
    let dir = TestDir::new();
    let (_, app) = open(&dir, "").await;
    let (_, mut rx) = connect(&app).await;
    app.observe_positions("follower", vec![]).await.unwrap();
    app.handle_event(event("place", "pending", TradeAction::PlaceLimit))
        .await
        .unwrap();
    app.pump().await.unwrap();
    let place = command(&mut rx);
    assert_eq!(place.action, TradeAction::PlaceLimit);
    app.handle_frame("follower", AgentFrame::Ack(ack(&place, Some("ticket"))))
        .await
        .unwrap();
    app.observe_positions(
        "follower",
        vec![BrokerPosition {
            id: "ticket".to_owned(),
            lots: 0.1,
            copier_managed: true,
            pending: true,
        }],
    )
    .await
    .unwrap();
    app.handle_event(event("amend", "pending", TradeAction::AmendPending))
        .await
        .unwrap();
    app.pump().await.unwrap();
    let amend = command(&mut rx);
    assert_eq!(amend.target_order_id.as_deref(), Some("ticket"));
    app.handle_frame("follower", AgentFrame::Ack(ack(&amend, Some("ticket"))))
        .await
        .unwrap();
    app.handle_event(event("cancel", "pending", TradeAction::CancelPending))
        .await
        .unwrap();
    app.pump().await.unwrap();
    let cancel = command(&mut rx);
    assert_eq!(cancel.action, TradeAction::CancelPending);
    assert_eq!(cancel.target_order_id.as_deref(), Some("ticket"));
    app.handle_frame("follower", AgentFrame::Ack(ack(&cancel, Some("ticket"))))
        .await
        .unwrap();
    assert_eq!(app.status().await["pending_mirrors"], 0);
}

#[tokio::test]
async fn follower_drift_blocks_queued_commands_until_mirror_is_reviewed() {
    let dir = TestDir::new();
    let (_, app) = open(&dir, "").await;
    let (_, mut rx) = connect(&app).await;
    app.handle_event(event("open", "old", TradeAction::Open))
        .await
        .unwrap();
    app.pump().await.unwrap();
    let open = command(&mut rx);
    app.handle_frame("follower", AgentFrame::Ack(ack(&open, Some("missing"))))
        .await
        .unwrap();
    app.observe_positions("follower", vec![]).await.unwrap();
    app.handle_event(event("next", "next", TradeAction::Open))
        .await
        .unwrap();
    app.pump().await.unwrap();
    assert!(rx.try_recv().is_err());
    app.set_paused(true).await.unwrap();
    app.review_mirror(MirrorReview {
        route_id: "copy".to_owned(),
        source_id: "old".to_owned(),
        target_id: None,
        source_lots: None,
        pending: false,
        reason: "Confirmed the manual close in broker history".to_owned(),
    })
    .await
    .unwrap();
    app.set_paused(false).await.unwrap();
    app.pump().await.unwrap();
    assert_eq!(command(&mut rx).source_order_id, "next");
    assert_eq!(
        app.status().await["recent_reviews"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

// ---- 23 Nov 2026 review fixes ----

/// The standard test config with extra route settings (the route table is last, so `route_extra` lands inside [[routes]]).
async fn open_route(dir: &TestDir, extra: &str, route_extra: &str) -> (Arc<Journal>, AppState) {
    let text = format!(
        r#"
durability = "fsync"
pending_routes = ["copy"]
{extra}
[[accounts]]
id = "master"
platform = "trinity"
role = "master"
token = "test"
[[accounts]]
id = "follower"
platform = "mt5"
role = "follower"
token = "test"
[[routes]]
id = "copy"
source_account_id = "master"
target_account_id = "follower"
{route_extra}
"#
    );
    let config: Arc<DaemonConfig> = Arc::new(toml::from_str(&text).unwrap());
    config.validate().unwrap();
    let (journal, replay) = Journal::open(dir.journal(), config.durability).await.unwrap();
    let journal = Arc::new(journal);
    (journal.clone(), AppState::new(config, journal, replay).unwrap())
}

/// Opens `order` on the follower and acknowledges it as `broker_id`.
async fn copy_open(app: &AppState, rx: &mut mpsc::Receiver<ServerFrame>, order: &str, broker_id: &str) {
    app.handle_event(event(&format!("open-{order}"), order, TradeAction::Open)).await.unwrap();
    app.pump().await.unwrap();
    let open = command(rx);
    assert_eq!(open.source_order_id, order);
    app.handle_frame("follower", AgentFrame::Ack(ack(&open, Some(broker_id)))).await.unwrap();
}

fn has_alert(status: &serde_json::Value, kind: &str) -> bool {
    status["alerts"].as_array().unwrap().iter().any(|alert| alert["kind"] == kind)
}

#[tokio::test]
async fn switching_a_route_off_still_closes_its_copies() {
    let dir = TestDir::new();
    let (journal, app) = open_route(&dir, "", "").await;
    let (_, mut rx) = connect(&app).await;
    copy_open(&app, &mut rx, "a", "broker-a").await;
    copy_open(&app, &mut rx, "b", "broker-b").await;
    // "a" is closed at the source while dispatch is paused: its close is still queued when the route is switched off
    app.set_paused(true).await.unwrap();
    app.handle_event(event("close-a", "a", TradeAction::Close)).await.unwrap();
    drop(app);
    drop(journal);

    let (_, app) = open_route(&dir, "", "enabled = false").await;
    let (_, mut rx) = connect(&app).await;
    app.set_paused(false).await.unwrap();
    app.pump().await.unwrap();
    let close_a = command(&mut rx);
    assert_eq!((close_a.action, close_a.target_order_id.as_deref()), (TradeAction::Close, Some("broker-a")));
    app.handle_frame("follower", AgentFrame::Ack(ack(&close_a, Some("broker-a")))).await.unwrap();

    // "b" is closed at the source after the route is off: the close is planned and sent all the same
    app.handle_event(event("close-b", "b", TradeAction::Close)).await.unwrap();
    app.pump().await.unwrap();
    let close_b = command(&mut rx);
    assert_eq!((close_b.action, close_b.target_order_id.as_deref()), (TradeAction::Close, Some("broker-b")));
    app.handle_frame("follower", AgentFrame::Ack(ack(&close_b, Some("broker-b")))).await.unwrap();

    // and the switched-off route opens nothing new
    app.handle_event(event("open-c", "c", TradeAction::Open)).await.unwrap();
    app.pump().await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(app.status().await["mirrors"], 0);
}

#[tokio::test]
async fn removing_a_route_with_open_copies_refuses_to_start() {
    let dir = TestDir::new();
    let (journal, app) = open_route(&dir, "", "").await;
    let (_, mut rx) = connect(&app).await;
    copy_open(&app, &mut rx, "a", "broker-a").await;
    drop(app);
    drop(journal);

    let config: Arc<DaemonConfig> = Arc::new(
        toml::from_str(
            r#"
durability = "fsync"
[[accounts]]
id = "master"
platform = "trinity"
role = "master"
token = "t"
[[accounts]]
id = "follower"
platform = "mt5"
role = "follower"
token = "t"
"#,
        )
        .unwrap(),
    );
    let (journal, replay) = Journal::open(dir.journal(), config.durability).await.unwrap();
    let error = AppState::new(config, Arc::new(journal), replay).err().expect("start refused");
    assert!(error.to_string().contains("route copy was removed"), "{error}");
}

#[tokio::test]
async fn full_backlog_keeps_a_close_for_later() {
    let dir = TestDir::new();
    let (_, app) = open_route(&dir, "backlog_capacity = 1", "").await;
    let (_, mut rx) = connect(&app).await;
    copy_open(&app, &mut rx, "a", "broker-a").await;
    // one queued open fills the backlog
    app.set_paused(true).await.unwrap();
    app.handle_event(event("open-x", "x", TradeAction::Open)).await.unwrap();
    app.handle_event(event("close-a", "a", TradeAction::Close)).await.unwrap();
    let status = app.status().await;
    assert_eq!(status["deferred"], 1, "the close is kept, not dropped");
    assert!(status["accounts"][1]["hold"].is_string(), "the follower is held");

    app.set_hold("follower", None).await.unwrap();
    app.set_paused(false).await.unwrap();
    app.pump().await.unwrap();
    let open_x = command(&mut rx);
    assert_eq!(open_x.source_order_id, "x");
    app.handle_frame("follower", AgentFrame::Ack(ack(&open_x, Some("broker-x")))).await.unwrap();
    app.pump().await.unwrap();
    let close = command(&mut rx);
    assert_eq!((close.action, close.target_order_id.as_deref()), (TradeAction::Close, Some("broker-a")));
}

#[tokio::test]
async fn copy_without_its_protection_stays_bound_and_flagged_until_a_stop_lands() {
    let dir = TestDir::new();
    let (_, app) = open_route(&dir, "", "").await;
    let (_, mut rx) = connect(&app).await;
    let mut opened = event("open-a", "a", TradeAction::Open);
    opened.stop_loss = Some(1.07);
    app.handle_event(opened).await.unwrap();
    app.pump().await.unwrap();
    let open = command(&mut rx);
    assert_eq!(open.stop_loss, Some(1.07));
    // the venue filled the order (acknowledged with its position ID) but its SL/TP request failed
    app.handle_frame("follower", AgentFrame::Ack(ack(&open, Some("broker-a")))).await.unwrap();
    app.protection_missing(&open, "broker-a", "SL/TP request refused").await.unwrap();
    let status = app.status().await;
    assert_eq!(status["mirrors"], 1, "the copy is bound, so the source's close will reach it");
    assert!(has_alert(&status, "protection_missing"));

    // a later stop update that succeeds clears the flag
    let mut modify = event("modify-a", "a", TradeAction::Modify);
    modify.stop_loss = Some(1.071);
    app.handle_event(modify).await.unwrap();
    app.pump().await.unwrap();
    let amend = command(&mut rx);
    assert_eq!((amend.action, amend.stop_loss), (TradeAction::Modify, Some(1.071)));
    app.handle_frame("follower", AgentFrame::Ack(ack(&amend, Some("broker-a")))).await.unwrap();
    let status = app.status().await;
    assert!(!has_alert(&status, "protection_missing"));
    assert_eq!(status["unprotected"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn unprotected_flag_survives_a_restart_and_clears_when_the_copy_closes() {
    let dir = TestDir::new();
    let (journal, app) = open_route(&dir, "", "").await;
    let (_, mut rx) = connect(&app).await;
    app.handle_event(event("open-a", "a", TradeAction::Open)).await.unwrap();
    app.pump().await.unwrap();
    let open = command(&mut rx);
    app.handle_frame("follower", AgentFrame::Ack(ack(&open, Some("broker-a")))).await.unwrap();
    app.protection_missing(&open, "broker-a", "refused").await.unwrap();
    drop(app);
    drop(journal);

    let (_, app) = open_route(&dir, "", "").await;
    let (_, mut rx) = connect(&app).await;
    assert!(has_alert(&app.status().await, "protection_missing"), "kept across a restart");
    app.handle_event(event("close-a", "a", TradeAction::Close)).await.unwrap();
    app.pump().await.unwrap();
    let close = command(&mut rx);
    assert_eq!(close.target_order_id.as_deref(), Some("broker-a"));
    app.handle_frame("follower", AgentFrame::Ack(ack(&close, Some("broker-a")))).await.unwrap();
    assert_eq!(app.status().await["unprotected"].as_array().unwrap().len(), 0);
}
