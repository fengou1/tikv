// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError,
    net::{SocketAddr},
    sync::{Arc, mpsc},
    str::FromStr,
    result,
};

use std::collections::HashMap;
use futures::executor::block_on;
use std::sync::mpsc::{sync_channel, SyncSender};
use futures::sink::SinkExt;
use futures::stream::{self, Stream, StreamExt};
use tikv::config::TiKvConfig;
use encryption_export::data_key_manager_from_config;
use engine_rocks::raw_util::new_engine_opt;
use engine_rocks::{RocksEngine};
use raft_log_engine::RaftLogEngine;
use engine_traits::{
    Engines, Iterable, Peekable, RaftEngine, CF_RAFT,
};

use grpcio::{ChannelBuilder, Environment, RequestStream, RpcContext, ServerBuilder, WriteFlags};
use kvproto::raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState};

use kvproto::recoverymetapb_grpc::{RecoveryMeta, create_recovery_meta};
use kvproto::recoverymetapb::{self, *};
use thiserror::Error;
use crate::Leader;

// TODO ERROR code may need more specific
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument {0:?}")]
    InvalidArgument(String),

    #[error("Not Found {0:?}")]
    NotFound(String),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}


pub fn start_recovery(config: TiKvConfig) -> Result<Vec<Leader>, Error>
{
    // TODO init a separete log file for recording meta and recovery meta log.
    //let _guard = log_util::init_log(None);
    let env = Arc::new(Environment::new(1));
    let (tx, rx) = sync_channel::<HashMap::<u64, RecoveryCmdRequest>>(1);

    let db_config = config.clone();
    let recovery = RecoverMetaSerivce::new(db_config, tx);
    let service = create_recovery_meta(recovery);
    
    // TODO, shall we limit the memory usage
    //let quota = ResourceQuota::new(Some("RecoveryMetaServerQuota")).resize_memory(1024 * 1024);
    let ch_builder = ChannelBuilder::new(env.clone());

    let addr = SocketAddr::from_str(config.server.addr.as_ref()).unwrap();
    let ip = format!("{}", addr.ip());

    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(&ip, addr.port())
        .channel_args(ch_builder.build_args())
        .build()
        .unwrap();
        server.start();
        for (host, port) in server.bind_addrs() {
            println!("listening on {}:{}", host, port);
        }

        // wait the force leaders list is generated
        let cmds = match rx.recv() {
            //TODO, what about the leaders list is None
            Ok(x) => x,
            Err(e) => {
                println!("get leader list failure.");
                return Err(Error::NotFound(format!("Received leader error.")));
            }
        };

        // shutdown the service
        let _ = block_on(server.shutdown());
        // TODO: part I, do some handling for local engine
        // TODO: part II, return the leader list to TiKV and start TiKV to force leader.
        let mut leaders = Vec::new();
        for (idx, cmd) in cmds.iter() {
            if cmd.as_leader {
                leaders.push(Leader{region_id:cmd.get_region_id(), commit_index: cmd.get_leader_commit_index(),});
            }
        }
        Ok(leaders)
}

#[derive(Clone)]
pub struct RecoverMetaSerivce {
    config: TiKvConfig,
    tx: SyncSender<HashMap::<u64, RecoveryCmdRequest>>,
    //engine_service: Box<dyn LocalEngineService>,

}


impl RecoverMetaSerivce {
    pub fn new(config: TiKvConfig, tx: SyncSender<HashMap::<u64, RecoveryCmdRequest>>) -> RecoverMetaSerivce{

        RecoverMetaSerivce {
            config: config,
            tx: tx,
            //engine_service: Box::new(local_engine_service),
        }
    }
}

impl RecoveryMeta for RecoverMetaSerivce {
    fn read_region_meta(&mut self, ctx: ::grpcio::RpcContext, _req: ReadRegionMetaRequest, mut sink: ::grpcio::ServerStreamingSink<RegionMeta>) {
        // implement a service method for BR to read the region meta
        println!("start to response the region meta");
        let local_engine_service = create_local_engine_service(&self.config);
        let region_meta = local_engine_service.unwrap().get_local_region_meta();

        let task = async move {
            let mut metas = stream::iter(
                region_meta
                    .into_iter()
                    .map(|x| Ok((x, WriteFlags::default()))),
            );

            // TODO Error handling necessary
            if let Err(e) = sink.send_all(&mut metas).await {
                println!("send meta: {:?}", e);
                return;
            }
            let _ = sink.close().await;
        };

        ctx.spawn(task);

    }
    fn recovery_cmd(&mut self, ctx: ::grpcio::RpcContext, mut stream: ::grpcio::RequestStream<RecoveryCmdRequest>, sink: ::grpcio::ClientStreamingSink<RecoveryCmdResponse>) {
        println!("start to recovery the region meta");
        //wait for the recovery command to recover the raft state, late on, we could bring all valid region back online to algin the log.
        //TODO: the raft state stay in normal states, but the raft index/term looks invalid, it may need to modified the rafe state.
        let tx = self.tx.clone();

        let task = async move {
            let mut leaders = HashMap::<u64, RecoveryCmdRequest>::default();
            while let Some(req) = stream.next().await {
                let req = req.map_err(|e| eprintln!("rpc recv fail: {}", e)).unwrap();
                assert!(leaders.insert(req.region_id, req).is_none());
            }

            tx.send(leaders).unwrap();
            let _ = sink.success(RecoveryCmdResponse::default()).await;
        };

        ctx.spawn(task);
    }
}

// BR will get all information for this to decided which peer shall be tombstone

/// Describes the meta information of a Local Region Peer.
#[derive(PartialEq, Debug, Default)]
pub struct LocalRegion {
    pub raft_local_state: RaftLocalState,
    pub raft_apply_state: RaftApplyState,
    pub region_local_state: RegionLocalState,
}

impl LocalRegion {
    fn new(
        raft_local: RaftLocalState,
        raft_apply: RaftApplyState,
        region_local: RegionLocalState,
    ) -> Self {
        LocalRegion {
            raft_local_state: raft_local,
            raft_apply_state: raft_apply,
            region_local_state: region_local,
        }
    }

    // fetch local region info into a gRPC message structure RegionMeta
    fn to_region_meta(&self) -> RegionMeta {
        // let old_region_local_state = self.raft_apply_state.ok_or_else(|| {
        //     Error::Other(format!("No RegionLocalState found for region {}", region_id).into())
        // })?;

        // TODO: unwrap may panic, better to have a check unwrap_or_xxx/ok_or_else
        let mut region_meta = RegionMeta::default();
        region_meta.region_id = self.region_local_state.get_region().id;
        region_meta.version = self.region_local_state.get_region().get_region_epoch().version;
        region_meta.tombstone = self.region_local_state.state == PeerState::Tombstone;
        region_meta.start_key = self.region_local_state.get_region().get_start_key().to_owned();
        region_meta.end_key = self.region_local_state.get_region().get_end_key().to_owned();

        region_meta.applied_index = self.raft_apply_state.applied_index;
        region_meta.last_index = self.raft_local_state.last_index;
        region_meta.term = self.raft_local_state.get_hard_state().term;

        return region_meta;
    } 
}


// block-level sst reader to init engine and read local region peer info
pub struct LocalEngines<ER: RaftEngine> {
    engines: Engines<RocksEngine, ER>,
}

impl<ER: RaftEngine> LocalEngines<ER> {
    pub fn new(
        engines: Engines<RocksEngine, ER>,
    ) -> LocalEngines<ER> {
        LocalEngines {
            engines,
        }
    }

    pub fn get_engine(&self) -> &Engines<RocksEngine, ER> {
        &self.engines
    }

    /// Get all regions holding region meta data from raft CF in KV storage.
    pub fn get_all_regions_in_store(&self) -> Result<Vec<u64>, Error> {
        let db = &self.engines.kv;
        let cf = CF_RAFT;
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let mut regions = Vec::with_capacity(128);
        box_try!(db.scan_cf(cf, start_key, end_key, false, |key, _| {
            let (id, suffix) = box_try!(keys::decode_region_meta_key(key));
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }
            regions.push(id);
            Ok(true)
        }));
        regions.sort_unstable();
        Ok(regions)
    }

    // get a local region info from a id
    fn region_info(&self, region_id: u64) -> Result<LocalRegion, Error> {
        let raft_state = box_try!(self.engines.raft.get_raft_state(region_id));

        let apply_state_key = keys::apply_state_key(region_id);
        let apply_state = box_try!(
            self.engines
                .kv
                .get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key)
        );

        let region_state_key = keys::region_state_key(region_id);
        let region_state = box_try!(
            self.engines
                .kv
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        );

        match (raft_state, apply_state, region_state) {
            (None, None, None) => Err(Error::NotFound(format!("info for region {}", region_id))),
            (raft_state, apply_state, region_state) => {
                Ok(LocalRegion::new(raft_state.unwrap(), apply_state.unwrap(), region_state.unwrap()))
            }
        }
    }

    // get all local region info from kv engine
    fn get_regions_info(&self) -> Vec<LocalRegion> {
        let region_ids =  self.get_all_regions_in_store().unwrap();

        // TODO: shall be const as macro and check if 1024*1024 is good
        let mut region_objects = Vec::with_capacity(1024 * 1024); 
        for region_id in region_ids {
            let r = self.region_info(region_id);

            //skip tombstone, TOD: where if we need skip?
            let region_state = &r.as_ref().unwrap().region_local_state;
            if region_state.get_state() == PeerState::Tombstone {
                continue;
            }
            
            region_objects.push(r.unwrap());
        }
        region_objects
    }

}

// the service to operator the local data
pub trait LocalEngineService {
    // the function is to read region meta from rocksdb and raft engine
    fn get_local_region_meta(&self) -> Vec<RegionMeta>;
    //fn recovery_raft_state(&self);
}


impl<ER: RaftEngine>  LocalEngineService for LocalEngines<ER> {
    // the function is to read region meta from rocksdb and raft engine
    fn get_local_region_meta(&self) -> Vec<RegionMeta> {
        // read the local region info
        let local_regions = self.get_regions_info();
        let region_metas = local_regions
        .iter()
        .map(|x| x.to_region_meta())
        .collect::<Vec<_>>();

        println!("region metas to report:");
        for meta in &region_metas {
            println!("\t{:?}", meta);
            println!("{}", meta.get_tombstone());
        }
        
        return region_metas;
    }
}

// create a engine reader, before that we need init engines firstly
// Notice: raft log engine could be a raft engine or rocksdb
pub fn create_local_engine_service(config: &TiKvConfig) -> Result<Box<dyn LocalEngineService>, String> {
    // init env for init kv db and raft engine
    let key_manager =
        data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
            .map_err(|e| format!("init encryption manager: {}", e))?
            .map(Arc::new);
    let env = config
        .build_shared_rocks_env(key_manager.clone(), None)
        .map_err(|e| format!("build shared rocks env: {}", e))?;
    let block_cache = config.storage.block_cache.build_shared_cache();

    // init rocksdb / kv db
    let mut db_opts = config.rocksdb.build_opt();
    db_opts.set_env(env.clone());
    let cf_opts = config
        .rocksdb
        .build_cf_opts(&block_cache, None, config.storage.api_version());
    let db_path = config
        .infer_kv_engine_path(None)
        .map_err(|e| format!("infer kvdb path: {}", e))?;
    println!("kvdb path: {:?}", db_path);
    let kv_db =
        new_engine_opt(&db_path, db_opts, cf_opts).map_err(|e| format!("create kvdb: {}", e))?;

    let mut kv_db = RocksEngine::from_db(Arc::new(kv_db));
    let shared_block_cache = block_cache.is_some();
    kv_db.set_shared_block_cache(shared_block_cache);

    // init raft engine, either is rocksdb or raft engine
    if !config.raft_engine.enable { // rocksdb
        let mut raft_db_opts = config.raftdb.build_opt();
        raft_db_opts.set_env(env);
        let raft_db_cf_opts = config.raftdb.build_cf_opts(&block_cache);
        let raft_path = config.infer_raft_db_path(None).map_err(|e| format!("infer raftdb path: {}", e))?;
        println!("raftdb path: {:?}", raft_path);
        let raft_db = new_engine_opt(&raft_path, raft_db_opts, raft_db_cf_opts).map_err(|e| format!("create kvdb: {}", e))?;
        let mut raft_db = RocksEngine::from_db(Arc::new(raft_db));
        raft_db.set_shared_block_cache(shared_block_cache);
        let local_engines = LocalEngines::new(Engines::new(kv_db, raft_db));
        Ok(Box::new(local_engines) as Box<dyn LocalEngineService>)
    } else { // raft engine
        let mut cfg = config.raft_engine.config();
        cfg.dir = config.infer_raft_engine_path(None).unwrap();
        if !RaftLogEngine::exists(&cfg.dir) {
            println!("raft engine not exists: {}", cfg.dir);
            tikv_util::logger::exit_process_gracefully(-1);
        }
        println!("raft engine path: {:?}", cfg.dir);
        let raft_db = RaftLogEngine::new(cfg, key_manager, None /*io_rate_limiter*/).unwrap();
        let local_engines = LocalEngines::new(Engines::new(kv_db, raft_db));
        Ok(Box::new(local_engines) as Box<dyn LocalEngineService>)
   }
}

