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
use grpcio::{
    Error as GrpcError, RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink,
    RequestStream,ClientStreamingSink,
};
use kvproto::{
    recoverypb::{self, *},
};
use raftstore::{
    router::RaftStoreRouter,
    store::msg::{Callback, RaftCmdExtraOpts},
};
use tikv_util::metrics;

/// Service handles the recovery messages from backup restore.
#[derive(Clone)]
pub struct RecoveryService<ER: RaftEngine, T: RaftStoreRouter<RocksEngine>> {
    threads: ThreadPool,
    engines: Engines<RocksEngine, ER>,
    raft_router: T,
   // primitive_regions: ReadRegionMetaResponse,
}

impl<ER: RaftEngine, T: RaftStoreRouter<RocksEngine>> RecoveryService<ER, T> {
    /// Constructs a new `Service` with `Engines`, a `RaftStoreRouter` and a `GcWorker`.
    pub fn new(
        engines: Engines<RocksEngine, ER>,
        raft_router: T,
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
        }
    }

    // // only for unary sink
    // fn handle_response<F, P>(
    //     &self,
    //     ctx: RpcContext<'_>,
    //     sink: UnarySink<P>,
    //     resp: F,
    //     tag: &'static str,
    // ) where
    //     P: Send + 'static,
    //     F: Future<Output = Result<P>> + Send + 'static,
    // {
    //     let ctx_task = async move {
    //         match resp.await {
    //             Ok(resp) => sink.success(resp).await?,
    //             Err(e) => sink.fail(error_to_status(e)).await?,
    //         }
    //         Ok(())
    //     };
    //     ctx.spawn(ctx_task.unwrap_or_else(move |e| on_grpc_error(tag, &e)));
    // }
}

impl<ER: RaftEngine, T: RaftStoreRouter<RocksEngine> + 'static> recoverypb::Recovery for RecoveryService<ER, T> {
    fn read_region_meta(&mut self, ctx: RpcContext, _req: ReadRegionMetaRequest, _sink: ServerStreamingSink<ReadRegionMetaResponse>) {
        // TODO: implement a read region meta inteface
        let handle_task = async move {
            println!("read_region_meta started");
        };
        self.threads.spawn_ok(handle_task);
    }
    fn recovery_cmd(&mut self, ctx: RpcContext, _stream: RequestStream<RecoveryCmdRequest>, _sink: ClientStreamingSink<RecoveryCmdResponse>) {
        // TODO: implement a recovery command inteface
        let handle_task = async move {
            println!("recovery_cmd started");
        };
        self.threads.spawn_ok(handle_task);
    }
    fn check_raft_status(&mut self, ctx: RpcContext, _req: GetRaftStatusRequest, _sink: UnarySink<GetRaftStatusResponse>) {
        // TODO: implement a check raft status inteface
        let handle_task = async move {
            println!("check_raft_status started");
        };
        self.threads.spawn_ok(handle_task);

    }
    fn resolved_kv_data(&mut self, ctx: RpcContext, _req: ResolvedRequest, _sink: UnarySink<ResolvedResponse>) {
        // TODO: implement a resolve/delete data funciton
        let handle_task = async move {
            println!(" resovled_kv_data started")
        };
        self.threads.spawn_ok(handle_task);
    }
}