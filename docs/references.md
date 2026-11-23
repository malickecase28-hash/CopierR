# Research references

The clones are under the sibling `research` directory. The implementations in CopierR use its existing code and vendor protocol contracts. No source from these references was imported.

| Repository | Reviewed revision | Top-level license | Reference scope |
| --- | --- | --- | --- |
| [mt-copier-mt4-mt5](https://github.com/unmind1980/mt-copier-mt4-mt5) | `5059c17c8c24f6a00ab40d13de3b86f4ef9a497f` | MIT | Position snapshots and master/follower synchronization |
| [Kopitra](https://github.com/ngotraders/kopitra) | `83b2039d5416bcb53e316750ca87501859d78dbc` | MIT | Execution boundary and operational management |
| [PropCopy](https://github.com/Do-T224/propcopy) | `8a2e3375b2e4ae168a6224c11f5f1e2a3680be8b` | MIT | Symbol mapping, sizing and follower-side ledger |
| [MT5TradeCopier](https://github.com/bpareshgit/MT5TradeCopier) | `b163a42499cdbe03404ab14d6f3947e11c4abebb` | MIT | Terminal-based synchronization |
| [TradeCopia](https://github.com/gattasrikanth/tradecopia) | `b43f15560a50265825c0a27d3f29ef544257d117` | Apache-2.0 | Execution and interface boundaries |
| [ctrader-openapi-rust](https://github.com/raul-gherman/ctrader-openapi-rust) | `c889a7ed6e9bf3265172e484c4fc90eb026f15db` | MIT | cTrader payload definitions |
| [rust-low-latency-trading-lab](https://github.com/ayushmishra2005/rust-low-latency-trading-lab) | `87ab6d672c94ab354bbc8d565220621339cda693` | MIT | Bounded processing and latency measurement |
| [fxapis-examples](https://github.com/FXapis/fxapis-examples) | `9e80fbfc0953bc95f8a3400f20b4ef04a4354699` | MIT | Multi-account REST and client wrappers |
| [cargo-chef](https://github.com/lukemathwalker/cargo-chef) | `dd01a86e7427fe2ceebf0eec13d0c6ad7c21def8` | MIT OR Apache-2.0 | Build caching |
| [cross](https://github.com/cross-rs/cross) | `a9301e91f72bba3850460512685f6b16062015ff` | MIT OR Apache-2.0 | Cross-compilation |
| [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) | `875d3aa050975e468e4771686f1cdbf35adaeccb` | MIT | Cross-compilation |

## Broker contracts

- cTrader order requests, execution events and reconciliation use the [Open API message reference](https://help.ctrader.com/open-api/messages/) and [symbol and model definitions](https://help.ctrader.com/open-api/model-messages/).
- MetaApi request shapes and outcomes use the [trade REST API](https://metaapi.cloud/docs/client/restApi/api/trade/), [trade result schema](https://metaapi.cloud/docs/client/models/metatraderTradeResponse/), [order model](https://metaapi.cloud/docs/client/models/metatraderOrder/) and [symbol specifications](https://metaapi.cloud/docs/client/restApi/api/retrieveMarketData/readSymbolSpecification/).
