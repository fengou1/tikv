// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, result, sync::Arc, thread, time::Duration};

use encryption_export::data_key_manager_from_config;
use engine_rocks::{util::new_engine_opt, RocksEngine};
use engine_traits::{Engines, Error as EngineError, Peekable, RaftEngine, SyncMutable};
use kvproto::{metapb, raft_serverpb::StoreIdent};
use pd_client::{Error as PdError, PdClient};
use raft_log_engine::RaftLogEngine;
use raftstore::store::initial_region;
use thiserror::Error;
use tikv::{config::TikvConfig, server::config::Config as ServerConfig};
use tikv_util::config::{ReadableDuration, ReadableSize, VersionTrack};

const CLUSTER_BOOTSTRAPPED_MAX_RETRY: u64 = 60;
const CLUSTER_BOOTSTRAPPED_RETRY_INTERVAL: Duration = Duration::from_secs(3);
pub const LOCK_FILE_ERROR: &str = "IO error: While lock file";

#[allow(dead_code)]
// TODO: ERROR need more specific
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument {0:?}")]
    InvalidArgument(String),

    #[error("Not Found {0:?}")]
    NotFound(String),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub type Result<T> = result::Result<T, Error>;

// snapshot recovery
// recovery mode parameter
const SNAP_MAX_TIMEOUT: usize = 12 * 60 * 60;

// may deleted after ban the asksplit from PD
const MAX_REGION_SIZE: u64 = 1024;
const MAX_SPLIT_KEY: u64 = 1 << 31;

/// Run a TiKV server in recovery mode
/// recovery mode include:
/// 1. no election happen between raft group
/// 2. peer valid during a recovery time even without leader in its region
/// 3. PD can not put any peer into tombstone
/// 4. must ensure all region data with ts less than backup ts (below commit
/// index) are safe
pub fn enter_snap_recovery_mode(config: &mut TikvConfig) {
    // TOOD: if we do not have to restart TiKV, then, we need exit the recovery mode
    // and config the following parameter back disable some configuration to ban
    // raft election etc. For snapshot recovery, no raft peers can start a new
    // election.
    info!("adjust the raft configure and rocksdb config.");
    let bt = config.raft_store.raft_base_tick_interval.0;

    config.raft_store.raft_election_timeout_ticks = SNAP_MAX_TIMEOUT;
    config.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(4 * 60 * 60);
    // time to check if peer alive without the leader, will not check peer during
    // this time interval
    config.raft_store.peer_stale_state_check_interval =
        ReadableDuration(bt * 4 * SNAP_MAX_TIMEOUT as _);

    // duration allow a peer alive without leader in region, otherwise report the
    // metrics and show peer as abnormal
    config.raft_store.abnormal_leader_missing_duration =
        ReadableDuration(bt * 4 * SNAP_MAX_TIMEOUT as _);

    // duration allow a peer alive without leader in region, otherwise report the PD
    // and delete itself(peer)
    config.raft_store.max_leader_missing_duration =
        ReadableDuration(bt * 4 * SNAP_MAX_TIMEOUT as _);

    // for optimize the write
    config.raft_store.snap_generator_pool_size = 10;
    config.raft_store.snap_apply_batch_size = ReadableSize::mb(10);
    config.raft_store.hibernate_regions = false;

    // disable auto compactions during the restore
    // config.rocksdb.defaultcf.disable_auto_compactions = true;
    // config.rocksdb.writecf.disable_auto_compactions = true;
    // config.rocksdb.lockcf.disable_auto_compactions = true;
    // config.rocksdb.raftcf.disable_auto_compactions = true;

    // TODO: in recovery mode, rocksdb may need a optimize for data write, the way
    // we done bellow does not work in rocksdb.
    config.rocksdb.defaultcf.level0_stop_writes_trigger = Some(12800);
    config.rocksdb.defaultcf.level0_slowdown_writes_trigger = Some(6400);
    config.rocksdb.writecf.level0_stop_writes_trigger = Some(12800);
    config.rocksdb.writecf.level0_slowdown_writes_trigger = Some(6400);
    config.rocksdb.lockcf.level0_stop_writes_trigger = Some(12800);
    config.rocksdb.lockcf.level0_slowdown_writes_trigger = Some(6400);

    config.rocksdb.max_background_jobs = 32;
    // disable resolve ts during the recovery
    config.resolved_ts.enable = false;

    config.server.grpc_keepalive_timeout = ReadableDuration::secs(300);
    config.server.grpc_keepalive_time = ReadableDuration::secs(90);

    // Disable region split during recovering.
    config.coprocessor.region_max_size = Some(ReadableSize::gb(MAX_REGION_SIZE));
    config.coprocessor.region_split_size = ReadableSize::gb(MAX_REGION_SIZE);
    config.coprocessor.region_max_keys = Some(MAX_SPLIT_KEY);
    config.coprocessor.region_split_keys = Some(MAX_SPLIT_KEY);
}

// update the cluster_id and bootcluster in pd before tikv startup
pub fn start_recovery(config: TikvConfig, cluster_id: u64, pd_client: Arc<dyn PdClient>) {
    let local_engine_service = create_local_engine_service(&config)
        .unwrap_or_else(|e| panic!("create a local engine reader failure, error is {}", e));

    local_engine_service.set_cluster_id(cluster_id.clone());
    info!("update cluster id {} from pd in recovery mode", cluster_id);
    let store_id = local_engine_service.get_store_id().unwrap_or_else(|e| {
        panic!(
            "can not found the store id from boot storage, error is {:?}",
            e
        )
    });

    let server_config = Arc::new(VersionTrack::new(config.server.clone()));
    let _ = bootcluster(
        &server_config.value().clone(),
        cluster_id,
        store_id,
        pd_client,
    );
}

// since we do not recover pd store meta, we have to bootcluster from pd by
// first region.
fn bootcluster(
    cfg: &ServerConfig,
    cluster_id: u64,
    store_id: u64,
    pd_client: Arc<dyn PdClient>,
) -> Result<()> {
    // build a store from config for bootcluster
    let mut store = metapb::Store::default();
    store.set_id(store_id);
    if cfg.advertise_addr.is_empty() {
        store.set_address(cfg.addr.clone());
    } else {
        store.set_address(cfg.advertise_addr.clone())
    }
    if cfg.advertise_status_addr.is_empty() {
        store.set_status_address(cfg.status_addr.clone());
    } else {
        store.set_status_address(cfg.advertise_status_addr.clone())
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

    let mut labels = Vec::new();
    for (k, v) in &cfg.labels {
        let mut label = metapb::StoreLabel::default();
        label.set_key(k.to_owned());
        label.set_value(v.to_owned());
        labels.push(label);
    }

    store.set_labels(labels.into());

    // init a region to boot pd cluster.·
    let region_id = pd_client
        .alloc_id()
        .unwrap_or_else(|e| panic!("get allocate id for region failure, error is {:?}", e));
    let peer_id = pd_client
        .alloc_id()
        .unwrap_or_else(|e| panic!("get allocate id for peer failure, error is {:?}", e));
    debug!(
        "alloc first peer id for first region";
        "peer_id" => peer_id,
        "region_id" => region_id,
    );

    let region = initial_region(store_id, region_id, peer_id);

    // bootstrap cluster to pd
    let mut retry = 0;
    while retry < CLUSTER_BOOTSTRAPPED_MAX_RETRY {
        match pd_client.bootstrap_cluster(store.clone(), region.clone()) {
            Ok(_) => {
                info!("bootstrap cluster ok in recovery mode"; "cluster_id" => cluster_id);
                return Ok(());
            }
            Err(PdError::ClusterBootstrapped(_)) => match pd_client.get_region(b"") {
                Ok(first_region) => {
                    if region == first_region {
                        return Ok(());
                    } else {
                        info!(
                            "cluster is already bootstrapped in recovery mode; cluster_id {}",
                            cluster_id
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    warn!("bootstrap cluster failure; error is {:?}", e);
                }
            },
            Err(e) => error!(
                "bootstrap cluster failure, cluster_id {}, error is {:?}",
                cluster_id, e
            ),
        }
        retry += 1;
        thread::sleep(CLUSTER_BOOTSTRAPPED_RETRY_INTERVAL);
    }
    Err(box_err!("bootstrapped cluster failed"))
}
// the service to operator the local engines
pub trait LocalEngineService {
    fn set_cluster_id(&self, cluster_id: u64);
    fn get_store_id(&self) -> Result<u64>;
}

// init engine and read local engine info
pub struct LocalEngines<ER: RaftEngine> {
    engines: Engines<RocksEngine, ER>,
}

impl<ER: RaftEngine> LocalEngines<ER> {
    pub fn new(engines: Engines<RocksEngine, ER>) -> LocalEngines<ER> {
        LocalEngines { engines }
    }

    pub fn get_engine(&self) -> &Engines<RocksEngine, ER> {
        &self.engines
    }
}

impl<ER: RaftEngine> LocalEngineService for LocalEngines<ER> {
    fn set_cluster_id(&self, cluster_id: u64) {
        let res = self
            .get_engine()
            .kv
            .get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)
            .unwrap_or_else(|e| {
                panic!("there is not ident in store, error is {:?}", e);
            });

        if res.is_none() {
            return;
        }

        let mut ident = res.unwrap();
        ident.set_cluster_id(cluster_id);

        self.get_engine()
            .kv
            .put_msg::<StoreIdent>(keys::STORE_IDENT_KEY, &ident)
            .unwrap();
        self.engines.sync_kv().unwrap();
    }

    // return cluster id and store id for registry the store to PD
    fn get_store_id(&self) -> Result<u64> {
        let res = self
            .engines
            .kv
            .get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)
            .unwrap_or_else(|e| panic!("get store id failure, error is {:?}", e));

        let ident = res.unwrap();

        let store_id = ident.get_store_id();
        if store_id == 0 {
            error!("invalid store to report");
        }

        Ok(store_id)
    }
}

fn handle_engine_error(err: EngineError) -> ! {
    error!("error while open kvdb: {}", err);
    if let EngineError::Engine(msg) = err {
        if msg.state().contains(LOCK_FILE_ERROR) {
            error!(
                "LOCK file conflict indicates TiKV process is running. \
                Do NOT delete the LOCK file and force the command to run. \
                Doing so could cause data corruption."
            );
        }
    }

    tikv_util::logger::exit_process_gracefully(-1);
}

// raft log engine could be a raft engine or rocksdb
pub fn create_local_engine_service(
    config: &TikvConfig,
) -> std::result::Result<Box<dyn LocalEngineService>, String> {
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
    let mut kv_db = match new_engine_opt(&db_path, db_opts, cf_opts) {
        Ok(db) => db,
        Err(e) => handle_engine_error(e),
    };

    let shared_block_cache = block_cache.is_some();
    kv_db.set_shared_block_cache(shared_block_cache);

    // init raft engine, either is rocksdb or raft engine
    if !config.raft_engine.enable {
        // rocksdb
        let mut raft_db_opts = config.raftdb.build_opt();
        raft_db_opts.set_env(env);
        let raft_db_cf_opts = config.raftdb.build_cf_opts(&block_cache);
        let raft_path = config
            .infer_raft_db_path(None)
            .map_err(|e| format!("infer raftdb path: {}", e))?;
        let mut raft_db = match new_engine_opt(&raft_path, raft_db_opts, raft_db_cf_opts) {
            Ok(db) => db,
            Err(e) => handle_engine_error(e),
        };
        //       let mut raft_db = RocksEngine::from_db(Arc::new(raft_db));
        raft_db.set_shared_block_cache(shared_block_cache);

        let local_engines = LocalEngines::new(Engines::new(kv_db, raft_db));
        Ok(Box::new(local_engines) as Box<dyn LocalEngineService>)
    } else {
        // raft engine
        let mut cfg = config.raft_engine.config();
        cfg.dir = config.infer_raft_engine_path(None).unwrap();
        if !RaftLogEngine::exists(&cfg.dir) {
            error!("raft engine not exists: {}", cfg.dir);
            tikv_util::logger::exit_process_gracefully(-1);
        }
        let raft_db = RaftLogEngine::new(cfg, key_manager, None /* io_rate_limiter */).unwrap();
        let local_engines = LocalEngines::new(Engines::new(kv_db, raft_db));

        Ok(Box::new(local_engines) as Box<dyn LocalEngineService>)
    }
}
