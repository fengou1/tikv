// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use engine_traits::{Engines, Mutable, Iterable, WriteBatchExt,RaftEngine, CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::{
    executor::{ThreadPool, ThreadPoolBuilder, block_on},
};

use futures::stream::{self, StreamExt};
use std::collections::{HashSet};
use std::sync::mpsc::{sync_channel};

use grpcio::{
    RpcContext,
    UnarySink,
    RequestStream,
    ClientStreamingSink,

};
use kvproto::{
    recoverdatapb::{self, *},
};
use raftstore::{
    router::RaftStoreRouter,
    store::msg::{PeerMsg, Callback, SignificantMsg},
    store::peer::{UnsafeRecoveryForceLeaderSyncer,UnsafeRecoveryWaitApplySyncer},
    store::fsm::RaftRouter,
    store::transport::SignificantRouter,
};

use txn_types::Key;
use tikv::storage::mvcc::{Lock};

/// Service handles the recovery messages from backup restore.
#[derive(Clone)]
pub struct RecoveryService<ER: RaftEngine> {
    threads: ThreadPool,
    engines: Engines<RocksEngine, ER>,
    router: RaftRouter<RocksEngine, ER>,
}

impl<ER: RaftEngine> RecoveryService<ER> {
    /// Constructs a new `Service` with `Engines`, a `RaftStoreRouter` and a `thread pool`.
    pub fn new(
        engines: Engines<RocksEngine, ER>,
        router: RaftRouter<RocksEngine, ER>,
    ) -> RecoveryService<ER> {
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
            println!("new recovery service");
        RecoveryService {
            threads,
            engines,
            router,
        }
    }

    // it is simply to send a Campaign and the check the leader
    pub fn force_leader(&self, region_leaders: Vec<u64>) {
        let leaders = region_leaders.clone();
        let raft_router = self.router.clone();
        
        let th = std::thread::spawn(move || {
            println!("starting tikv success, start to force leader");
            //it looks more effective that we directly send a campaign to region and then check it by LeaderCallback.
            let mut rxs = Vec::with_capacity(leaders.len());
            for &region_id in &leaders {
                if let Err(e) = raft_router.significant_send(region_id, SignificantMsg::Campaign) {
                    //TODO: retry may necessay
                    println!("region {} fails to campaign: {}", region_id, e);
                    continue;
                } else {
                    println!("region {} starts to campaign", region_id);
                }
    
                let (tx, rx) = sync_channel(1);
                let cb = Callback::Read(Box::new(move |_| tx.send(1).unwrap()));
                raft_router
                    .significant_send(region_id, SignificantMsg::LeaderCallback(cb))
                    .unwrap();
                rxs.push(Some(rx));
            }
            
            // leader is campaign and be ensured as leader
            for (rid, rx) in leaders.iter().zip(rxs) {
                if let Some(rx) = rx {
                    let _ = rx.recv().unwrap();
                    println!("region {} is leader", rid);
                }
            }
            println!("all region state adjustments are done");
        });
        
        th.join().unwrap();
    }

    fn wait_apply_leaders(&self) {
        let router = self.router.clone();
        println!("apply leader log started");
        let wait_apply = UnsafeRecoveryWaitApplySyncer::new(0, router.clone(), true);
        router.broadcast_normal(|| {
        PeerMsg::SignificantMsg(SignificantMsg::UnsafeRecoveryWaitApply(wait_apply.clone()))
        });
    }
}

impl<ER: RaftEngine> recoverdatapb::RecoverData for RecoveryService<ER> {
    fn recover_cmd(&mut self, ctx: RpcContext, mut stream: RequestStream<RecoverCmdRequest>, sink: ClientStreamingSink<RecoverCmdResponse>) {
        println!("start to recovery the region meta");

        let mut leaders = Vec::new();
        let _res = block_on(async {
            while let Some(req) = stream.next().await {
                let req = req.map_err(|e| eprintln!("rpc recv fail: {}", e)).unwrap();
                if req.as_leader {
                    leaders.push(req.region_id);
                }
            }
        });

        self.force_leader(leaders.clone());
        self.wait_apply_leaders();
        ctx.spawn(async {
            let _ = sink.success(RecoverCmdResponse::default()).await;
        });

        return;
    }

    // currently we only delete the lock cf and write cf, the data cf will not delete in this version.
    fn resolve_kv_data(&mut self, _ctx: RpcContext, req: ResolveRequest, sink: UnarySink<ResolveResponse>)
    {
        // implement a resolve/delete data funciton
        let resolved_ts = req.get_resolved_ts();

        // resolved default cf by ts
        let kv_db = self.engines.kv.clone();
        let default_task = async move {
            println!(" resovled_kv_data started");
            // refer to restore_kv_meta, not sure if we can delete when we iterate the kv_db
            let mut kv_wb = kv_db.write_batch();
            kv_db.scan_cf(
                CF_LOCK,
                keys::DATA_MIN_KEY,
                keys::DATA_MAX_KEY,
                false,
                |key, _value| {
                    println!("lock key : {:?}", key);
                    kv_wb.delete(key).unwrap();
                    Ok(true)
                },
            )
            .unwrap();
        };

        // resolved write cf by ts
        let write_db = self.engines.kv.clone();
        let mut kv_wb = write_db.write_batch();
        let write_task = async move {
            println!(" resolve_kv_data started");
            write_db.scan_cf(
                CF_WRITE,
                keys::DATA_MIN_KEY,
                keys::DATA_MAX_KEY,
                false,
                |key, _value| {
                    let (_prefix, commit_ts) = box_try!(Key::split_on_ts_for(key));
        
                    if commit_ts.into_inner() <= resolved_ts {
                        // Skip stale records.
                        return Ok(true);
                    }
                    println!("write ts: {}", commit_ts.into_inner());
                    
                    kv_wb.delete_cf(CF_WRITE, key).unwrap();
                    Ok(true)
                },
            )
            .unwrap();
    
            let _ = sink.success(ResolveResponse::default()).await;
        };

        self.threads.spawn_ok(default_task);
        self.threads.spawn_ok(write_task);
    }
}