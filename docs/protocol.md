# CopierR wire protocol v1

CopierR uses UTF-8, newline-delimited, tab-separated frames over a local TCP
connection. Fields may not contain tabs, CR or LF. The Rust encoders sanitize
those characters.

The daemon listens on `127.0.0.1:48100` by default and sets `TCP_NODELAY`.
Every terminal/client must send `HELLO` before any other frame.

## Client to daemon

```text
HELLO  1  account_id  platform  token
EVENT  1  event_id  account_id  platform  action  source_order_id  symbol  side  volume  remaining_volume  price  sl  tp  timestamp_unix_ns  origin_command_id
ACK    1  command_id  account_id  status  external_id  timestamp_unix_ns  message
PING   1  timestamp_unix_ns
```

The separators above are literal TAB bytes.

Platforms: `trinity`, `mt4`, `mt5`, `ctrader`.

Actions: `open`, `modify`, `reduce`, `close`.

Sides: `buy`, `sell`, or empty when the action does not require direction.

ACK statuses: `accepted`, `filled`, `rejected`, `unknown`.

## Daemon to client

```text
WELCOME  1  server_time_unix_ns
COMMAND  1  command_id  origin_event_id  route_id  source_account_id  source_order_id  target_account_id  action  target_order_id  symbol  side  volume  source_volume  source_remaining_volume  price  sl  tp
ERROR    1  code  message
PONG     1  server_time_unix_ns
```

`target_order_id` is empty for an open command. For modify, reduce and close it
is the follower's external order/position identifier from the open ACK.

`volume` is normalized lots. The cTrader bridge converts lots to volume units
at the terminal boundary.

## Idempotency

Master bridges should make `event_id` deterministic for a broker state change.
CopierR derives `command_id` from the event ID, route ID and target account.
Duplicate master events therefore do not create duplicate commands.

Copied positions should carry the command ID in their terminal comment/label or
magic metadata. Bridges should not republish those changes unless the account
is explicitly configured with `allow_rebroadcast = true`.
