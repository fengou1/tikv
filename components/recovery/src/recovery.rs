// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, sync::Arc};
use tikv::config::TiKvConfig;
use encryption_export::data_key_manager_from_config;
use engine_rocks::raw_util::new_engine_opt;
use engine_rocks::{RocksEngine};
use raft_log_engine::RaftLogEngine;
use engine_traits::{
    Engines, Iterable, Peekable, RaftEngine, CF_RAFT,
};

use kvproto::raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState};
use kvproto::recoverypb::ReadRegionMetaResponse;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument {0:?}")]
    InvalidArgument(String),

    #[error("Not Found {0:?}")]
    NotFound(String),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
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
    fn to_region_meta(&self) -> ReadRegionMetaResponse {
        // let old_region_local_state = self.raft_apply_state.ok_or_else(|| {
        //     Error::Other(format!("No RegionLocalState found for region {}", region_id).into())
        // })?;

        // TODO: unwrap may panic, better to have a check unwrap_or_xxx/ok_or_else
        let mut region_meta = ReadRegionMetaResponse::default();
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

pub trait LocalEnginesReader {
    // the function is to read region meta from rocksdb and raft engine
    fn get_local_region_meta(&self) -> Vec<ReadRegionMetaResponse>;
}

impl<ER: RaftEngine>  LocalEnginesReader for LocalEngines<ER> {
    // the function is to read region meta from rocksdb and raft engine
    fn get_local_region_meta(&self) -> Vec<ReadRegionMetaResponse> {
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
pub fn new_reader(config: &TiKvConfig) -> Result<Box<dyn LocalEnginesReader>, String> {
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
        Ok(Box::new(local_engines) as Box<dyn LocalEnginesReader>)
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
        Ok(Box::new(local_engines) as Box<dyn LocalEnginesReader>)
   }
}
