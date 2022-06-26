// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use engine_traits::{Engines, Mutable, Iterable, WriteBatchExt,RaftEngine, CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::{
    executor::{ThreadPool, ThreadPoolBuilder},
};
use std::collections::{HashSet};
use std::sync::mpsc::{sync_channel};

use grpcio::{
    RpcContext,  UnarySink,
};
use kvproto::{
    recoverypb::{self, *},
};
use raftstore::{
    router::RaftStoreRouter,
    store::msg::{PeerMsg, Callback, SignificantMsg},
    store::peer::{UnsafeRecoveryForceLeaderSyncer,UnsafeRecoveryWaitApplySyncer},
    store::fsm::RaftRouter,
    store::transport::SignificantRouter,
};

use txn_types::Key;
use crate::Leader;
use tikv::storage::mvcc::{Lock};

/// Service handles the recovery messages from backup restore.
#[derive(Clone)]
pub struct RecoveryService<ER: RaftEngine> {
    threads: ThreadPool,
    engines: Engines<RocksEngine, ER>,
    router: RaftRouter<RocksEngine, ER>,
    region_leaders: Option<Vec<Leader>>,
}

impl<ER: RaftEngine> RecoveryService<ER> {
    /// Constructs a new `Service` with `Engines`, a `RaftStoreRouter` and a `thread pool`.
    pub fn new(
        engines: Engines<RocksEngine, ER>,
        router: RaftRouter<RocksEngine, ER>,
        region_leaders: Option<Vec<Leader>>,

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

        RecoveryService {
            threads,
            engines,
            router,
            region_leaders,
        }
    }

    // this is plan A, it is simply to send a Campaign and the check the leader
    pub fn force_leader(&self) {
        let region_leader = self.region_leaders.as_ref().unwrap();
        if region_leader.is_empty() {
            return;
        }

        let leaders = region_leader.clone();
        let raft_router = self.router.clone();
        
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

    // this is plan B, it is simply reused the unsafe online recovery, with failed store id = 0, the store id never be 0?
    pub fn force_leader_with_failed_store(&self) {
        let region_leader = self.region_leaders.as_ref().unwrap();
        if region_leader.is_empty() {
            return;
        }

        let leaders = region_leader.clone();
        let raft_router = self.router.clone();
        let router  = self.router.clone();
        // TODO, we need a correct way to handle the log
        let th = std::thread::spawn(move || {
            println!("starting tikv success, start to force leader");
            // Assume the store 0 is failed.
            let mut failed_stores = HashSet::default();
            failed_stores.insert(0);
            let syncer = UnsafeRecoveryForceLeaderSyncer::new(
                0, // no need the report
                router.clone(),
            );
            
            for leader in &leaders {

                if let Err(e) = raft_router.significant_send(
                    leader.region_id,
                    SignificantMsg::EnterForceLeaderState {
                        syncer: syncer.clone(),
                        failed_stores: failed_stores.clone(),
                    },
                ) {
                    println!("fail to send force leader message for recovery Error {}", e);
                }
            }
            println!("all region state adjustments are done");
        });
        
        th.join().unwrap();
    }
}

impl<ER: RaftEngine> recoverypb::Recovery for RecoveryService<ER> {
    fn check_raft_status(&mut self, _ctx: RpcContext, _req: GetRaftStatusRequest, sink: UnarySink<GetRaftStatusResponse>) {
        // TODO: implement a check raft status inteface
        let router = self.router.clone();
        let handle_task = async move {
            println!("check_raft_status started");

            let wait_apply = UnsafeRecoveryWaitApplySyncer::new(0, router.clone(), true);
            router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::UnsafeRecoveryWaitApply(wait_apply.clone()))
            });

            let rep = GetRaftStatusResponse::default();
            let _ = sink.success(rep).await;
        };
        self.threads.spawn_ok(handle_task);

    }
    // currently we only delete the lock cf and write cf, the data cf will not delete in this version.
    fn resolved_kv_data(&mut self, _ctx: RpcContext, req: ResolvedRequest, sink: UnarySink<ResolvedResponse>)
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
                |key, value| {
                    println!("lock key : {:?}", key);

                    let lock = box_try!(Lock::parse(value));
                    if lock.ts.into_inner() <= resolved_ts {
                        println!("the resolved_ts has issue during the backup");
                        // TODO: panic may necessary, here.
                        return Ok(true);
                    }

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
    
            let _ = sink.success(ResolvedResponse::default()).await;
        };

        self.threads.spawn_ok(default_task);
        self.threads.spawn_ok(write_task);
    }
}