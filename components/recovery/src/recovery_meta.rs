// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError,
    net::SocketAddr,
    sync::Arc,
    str::FromStr,
};

use futures::executor::block_on;
use std::sync::mpsc::{sync_channel, SyncSender};
use futures::sink::SinkExt;
use futures::stream::{self};
use tikv::config::TiKvConfig;
use encryption_export::data_key_manager_from_config;
use engine_rocks::raw_util::new_engine_opt;
use engine_rocks::{RocksEngine};
use raft_log_engine::RaftLogEngine;
use engine_traits::{
    Engines, Iterable, Peekable, RaftEngine, CF_RAFT,
};

use pd_client::{PdClient, RpcClient};
use grpcio::{ChannelBuilder,
    Environment,
    ServerBuilder,
    WriteFlags,
    RpcContext,
    ServerStreamingSink,
    };
use kvproto::raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent};
use kvproto::metapb;

use kvproto::recovermetapb_grpc::{RecoverMeta, create_recover_meta};
use kvproto::recovermetapb::{*};
use thiserror::Error;

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

// Notice: the start_recovery will start a exclusive gRPC serivce, the purpose is to handle the data before tikv node started
// Why not use TiKV gRPC service? gRPC service start too late, and really hard to reuse the exisited gRPC service.
pub fn start_recovery(config: TiKvConfig, pd_client: Arc<RpcClient>)
{
    // TODO init a separete log file for recording meta and recovery meta log.
    //let _guard = log_util::init_log(None);
    let env = Arc::new(Environment::new(1));
    let (tx, rx) = sync_channel::<bool>(1);

    let recovery = RecoverMetaSerivce::new(config.clone(), tx);
    let service = create_recover_meta(recovery);
    
    // registry the store to PD
    registry_store_to_pd(config.clone(), pd_client);
    // TODO, shall we limit the memory usage
    //let quota = ResourceQuota::new(Some("RecoverMetaServerQuota")).resize_memory(1024 * 1024);
    let ch_builder = ChannelBuilder::new(env.clone());

    let addr = SocketAddr::from_str(config.server.addr.as_ref()).unwrap();
    let ip = format!("{}", addr.ip());
    println!("stage: start recovery service before tikv fully start");
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

        // wait the exit service
        let _exit = match rx.recv() {
            Ok(x) => x,
            Err(_e) => {
                println!("ReadMeta Failed.");
                return;
            }
        };

        // shutdown the service, will this enough to release the socket resource?
        let _ = block_on(server.shutdown());
        println!("shutdown server");
}

// BR get the store info from PD when registery had been done
// the node start will update the store with more info
pub fn registry_store_to_pd(config: TiKvConfig, pd_client: Arc<RpcClient>) {
    println!("stage: register the store into pd service.");
    let server_config = config.clone();
    let local_engine_service = create_local_engine_service(&server_config); 
    let store_id = local_engine_service.unwrap().get_store_id().unwrap();
    let mut store = metapb::Store::default();
        store.set_id(store_id);
        if server_config.server.advertise_addr.is_empty() {
            store.set_address(server_config.server.addr.clone());
        } else {
            store.set_address(server_config.server.advertise_addr.clone())
        }
        if server_config.server.advertise_status_addr.is_empty() {
            store.set_status_address(server_config.server.status_addr.clone());
        } else {
            store.set_status_address(server_config.server.advertise_status_addr.clone())
        }
        store.set_version(env!("CARGO_PKG_VERSION").to_string());

        if let Ok(path) = std::env::current_exe() {
            if let Some(path) = path.parent() {
                store.set_deploy_path(path.to_string_lossy().to_string());
            }
        };

        store.set_start_timestamp(chrono::Local::now().timestamp());
        store.set_git_hash(
            option_env!("TIKV_BUILD_GIT_HASH")
                .unwrap_or("Unknown git hash")
                .to_string(),
        );

    println!("put_store, store id: {} store version: {}", store.get_id(), store.get_version());
    // put store is just update the store to pd, so that the br can get the tikv service from pd
    let _status = pd_client.put_store(store.clone());
}

#[derive(Clone)]
pub struct RecoverMetaSerivce {
    config: TiKvConfig,
    tx: SyncSender<bool>,
}


impl RecoverMetaSerivce {
    pub fn new(config: TiKvConfig, tx: SyncSender<bool>) -> RecoverMetaSerivce{
        //1. regrestry the store into PD, so that BR can reach the TiKV via PD GetAllStores
        RecoverMetaSerivce {
            config: config,
            tx: tx,
        }
    }

    pub fn recover_raft_state(&self){
        //TODO: the raft state stay in normal states, but the raft index/term looks invalid, it may need to modified the rafe state.
        println!("recover_raft_state");
    }
}

impl RecoverMeta for RecoverMetaSerivce {
    fn read_region_meta(&mut self, ctx: RpcContext, _req: ReadRegionMetaRequest, mut sink: ServerStreamingSink<RegionMeta>) {
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
                println!("send meta error: {:?}", e);
                return;
            }

            println!("meta sent already");
            let _ = sink.close().await;

        };
        ctx.spawn(task);
        
        let _ = self.tx.send(true);
    }

}

// the service to operator the local engines
pub trait LocalEngineService{
    // the function is to read region meta from rocksdb and raft engine
    fn get_local_region_meta(&self) -> Vec<RegionMeta>;
    //fn recovery_raft_state(&self);
    fn get_store_id(&self) -> Result<u64, String>;
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
    // return cluster id and store id for registry the store to PD
    fn get_store_id(&self) -> Result<u64, String> {
        let res = self.get_engine().kv.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)?;
        if res.is_none() {
            return Ok(0);
        }

        let ident = res.unwrap();
        let store_id = ident.get_store_id();
        if store_id == 0 {
            println!("invalid store to report");
        }

        Ok(store_id)
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

        region_meta.last_log_term = self.raft_local_state.get_hard_state().term;
        region_meta.last_index = self.raft_local_state.last_index;

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
