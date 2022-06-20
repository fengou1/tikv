// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use engine_traits::{Engines, MiscExt, RaftEngine};
use futures::{
    executor::{ThreadPool, ThreadPoolBuilder},
    channel::oneshot,
    future::{Future, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::{self, TryStreamExt},
};
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use std::thread;
use grpcio::{
    Error as GrpcError, RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink,
    RequestStream,ClientStreamingSink,
};
use kvproto::{
    recoverymetapb::{RegionMeta},
    recoverypb::{self, *},
};
use raftstore::{
    router::RaftStoreRouter,
    store::msg::{Callback, SignificantMsg},
};

use crate::Leader;

/// Service handles the recovery messages from backup restore.
#[derive(Clone)]
pub struct RecoveryService<ER: RaftEngine, T: RaftStoreRouter<RocksEngine>> {
    threads: ThreadPool,
    engines: Engines<RocksEngine, ER>,
    raft_router: T,
    region_leaders: Option<Vec<Leader>>,
}

impl<ER: RaftEngine, T: RaftStoreRouter<RocksEngine> + 'static> RecoveryService<ER, T> {
    /// Constructs a new `Service` with `Engines`, a `RaftStoreRouter` and a `thread pool`.
    pub fn new(
        engines: Engines<RocksEngine, ER>,
        raft_router: T,
        region_leaders: Option<Vec<Leader>>,

    ) -> RecoveryService<ER, T> {
        let props = tikv_util::thread_group::current_properties();
        let threads = ThreadPoolBuilder::new()
        // TODO: shall we do a const or config thread pool?
            .pool_size(4)
            .name_prefix("recovery-service")
            .after_start(move |_| {
                tikv_util::thread_group::set_properties(props.clone());
                tikv_alloc::add_thread_memory_accessor();
            })
            .before_stop(move |_| tikv_alloc::remove_thread_memory_accessor())
            .create()
            .unwrap();

        RecoveryService {
            threads,
            engines,
            raft_router,
            region_leaders,
        }
    }

    pub fn force_leader(&self) {
        let region_leader = self.region_leaders.as_ref().unwrap();
        if region_leader.is_empty() {
            return;
        }

        let leaders = region_leader.clone();
        let raft_router = self.raft_router.clone();
        
        // TODO, we need a correct way to handle the log
        let th = std::thread::spawn(move || {
            println!("starting tikv success, start to force leader");
    
            //TODO: it looks more effective that we directly send a campaign to region and check it by LeaderCallback.
            let mut rxs = Vec::with_capacity(leaders.len());
            for leader in &leaders {
                if let Err(e) = raft_router.significant_send(leader.region_id, SignificantMsg::Campaign) {
                    println!("region {} fails to campaign: {}", leader.region_id, e);
                    continue;
                } else {
                    println!("region {} starts to campaign", leader.region_id);
                }
    
                let (tx, rx) = sync_channel(1);
                let cb = Callback::Read(Box::new(move |_| tx.send(1).unwrap()));
                raft_router
                    .significant_send(leader.region_id, SignificantMsg::LeaderCallback(cb))
                    .unwrap();
                rxs.push(Some(rx));
            }
            // leader is campaign and be ensured as leader
            for (rid, rx) in leaders.iter().zip(rxs) {
                if let Some(rx) = rx {
                    let _ = rx.recv().unwrap();
                    println!("region {} is leader", rid.region_id);
                }
            }
    
            println!("all region state adjustments are done");
        });
        
        th.join().unwrap();
    }

}

impl<ER: RaftEngine, T: RaftStoreRouter<RocksEngine> + 'static> recoverypb::Recovery for RecoveryService<ER, T> {

    fn check_raft_status(&mut self, _ctx: RpcContext, _req: GetRaftStatusRequest, _sink: UnarySink<GetRaftStatusResponse>) {
        // TODO: implement a check raft status inteface
        let handle_task = async move {
            println!("check_raft_status started");
        };
        self.threads.spawn_ok(handle_task);

    }
    fn resolved_kv_data(&mut self, _ctx: RpcContext, _req: ResolvedRequest, _sink: UnarySink<ResolvedResponse>) {
        // TODO: implement a resolve/delete data funciton
        let handle_task = async move {
            println!(" resovled_kv_data started")
        };
        self.threads.spawn_ok(handle_task);
    }
}