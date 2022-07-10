// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: pleas check if the following definition is Ok in lib.rs, what about mod.rs?
pub mod recovery_meta;
pub mod recovery_service;
#[macro_use]
extern crate tikv_util;

//pub use recovery_meta::RecoverMetaSerivce;
pub use recovery_service::RecoveryService;
pub use recovery_meta::start_recovery;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
