// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::*, time::Duration};

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::{RocksEngine, RocksSnapshot};
use grpcio::{ChannelBuilder, ClientUnaryReceiver, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use online_config::ConfigValue;
use raftstore::coprocessor::CoprocessorHost;
use resolved_ts::{Observer, Task};
use test_raftstore::*;
use tikv::config::ResolvedTsConfig;
use tikv_util::{worker::LazyWorker, HandyRwLock};
use txn_types::TimeStamp;
static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(test_util::setup_for_ci);
}