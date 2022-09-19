// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{time::Duration};

use futures::StreamExt;
use raft::eraftpb::MessageType;
use raftstore::store::{PeerMsg, SignificantMsg, SnapshotRecoveryWaitApplySyncer};
use test_raftstore::*;
use tikv_util::HandyRwLock;

#[test]
fn test_check_pending_admin() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.store_io_pool_size = 0;

    cluster.run();

    // write a key to let leader stuck.
    cluster.must_put(b"k", b"v");
    must_get_equal(&cluster.get_engine(1), b"k", b"v");
    must_get_equal(&cluster.get_engine(2), b"k", b"v");
    must_get_equal(&cluster.get_engine(3), b"k", b"v");

    // add filter to make leader cannot commit apply
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
    ));

    // make a admin request to let leader has pending conf change.
    let leader = new_peer(1, 4);
    cluster.async_add_peer(1, leader).unwrap();

    std::thread::sleep(Duration::from_millis(800));

    let router = cluster.sim.wl().get_router(1).unwrap();

    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    router.broadcast_normal(|| {
        PeerMsg::SignificantMsg(SignificantMsg::CheckPendingAdmin(tx.clone()))
    });
    futures::executor::block_on(async {
        let r = rx.next().await;
        if let Some(r) = r {
            assert_eq!(r.has_pending_admin, true);
        }
    });

    // clear filter so we can make pending admin requests finished.
    cluster.clear_send_filters();

    std::thread::sleep(Duration::from_millis(800));

    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    router.broadcast_normal(|| {
        PeerMsg::SignificantMsg(SignificantMsg::CheckPendingAdmin(tx.clone()))
    });
    futures::executor::block_on(async {
        let r = rx.next().await;
        if let Some(r) = r {
            assert_eq!(r.has_pending_admin, false);
        }
    });
}

#[test]
fn test_snap_wait_apply() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.store_io_pool_size = 0;

    cluster.run();

    // write a key to let leader stuck.
    cluster.must_put(b"k", b"v");
    must_get_equal(&cluster.get_engine(1), b"k", b"v");
    must_get_equal(&cluster.get_engine(2), b"k", b"v");
    must_get_equal(&cluster.get_engine(3), b"k", b"v");

    // add filter to make leader 1 cannot receive follower append response.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
    ));

    // make a async put request to let leader has inflight raft log.
    cluster.async_put(b"k2", b"v2").unwrap();
    std::thread::sleep(Duration::from_millis(800));

    let router = cluster.sim.wl().get_router(1).unwrap();

    let (tx, rx) = std::sync::mpsc::sync_channel(1);

    router.broadcast_normal(|| {
        PeerMsg::SignificantMsg(SignificantMsg::SnapshotRecoveryWaitApply(
            SnapshotRecoveryWaitApplySyncer::new(1, tx.clone()),
        ))
    });

    // we expect recv timeout because the leader peer on store 1 cannot finished the
    // apply. so the wait apply will timeout.
    rx.recv_timeout(Duration::from_secs(1)).unwrap_err();

    // clear filter so we can make wait apply finished.
    cluster.clear_send_filters();
    std::thread::sleep(Duration::from_millis(800));

    // after clear the filter the leader peer on store 1 can finsihed the wait
    // apply.
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    router.broadcast_normal(|| {
        PeerMsg::SignificantMsg(SignificantMsg::SnapshotRecoveryWaitApply(
            SnapshotRecoveryWaitApplySyncer::new(1, tx.clone()),
        ))
    });

    // we expect recv the region id from rx.
    assert_eq!(rx.recv(), Ok(1));
}
