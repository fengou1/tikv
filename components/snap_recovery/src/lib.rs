// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub mod init_cluster;
pub mod services;
#[macro_use]
extern crate tikv_util;

pub use init_cluster::{enter_snap_recovery_mode, start_recovery};
pub use services::RecoveryService;

mod data_resolver;
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
