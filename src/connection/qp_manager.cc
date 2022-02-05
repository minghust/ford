// Author: Ming Zhang
// Copyright (c) 2021

#include "connection/qp_manager.h"

void QPManager::BuildQPConnection(MetaManager* meta_man) {
  for (const auto& remote_node : meta_man->remote_nodes) {
    // Note that each remote machine has one MemStore mr and one Log mr
    MemoryAttr remote_hash_mr = meta_man->GetRemoteHashMR(remote_node.node_id);
    MemoryAttr remote_log_mr = meta_man->GetRemoteLogMR(remote_node.node_id);

    // Build QPs with one remote machine (this machine can be a primary or a backup)
    // Create the thread local queue pair
    MemoryAttr local_mr = meta_man->global_rdma_ctrl->get_local_mr(CLIENT_MR_ID);
    RCQP* data_qp = meta_man->global_rdma_ctrl->create_rc_qp(create_rc_idx(remote_node.node_id, (int)global_tid * 2),
                                                             meta_man->opened_rnic,
                                                             &local_mr);

    RCQP* log_qp = meta_man->global_rdma_ctrl->create_rc_qp(create_rc_idx(remote_node.node_id, (int)global_tid * 2 + 1),
                                                            meta_man->opened_rnic,
                                                            &local_mr);

    // Queue pair connection, exchange queue pair info via TCP
    ConnStatus rc;
    do {
      rc = data_qp->connect(remote_node.ip, remote_node.port);
      if (rc == SUCC) {
        data_qp->bind_remote_mr(remote_hash_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
        data_qps[remote_node.node_id] = data_qp;
        // RDMA_LOG(INFO) << "Thread " << global_tid << ": Data QP connected! with remote node: " << remote_node.node_id << " ip: " << remote_node.ip;
      }
      usleep(2000);
    } while (rc != SUCC);

    do {
      rc = log_qp->connect(remote_node.ip, remote_node.port);
      if (rc == SUCC) {
        log_qp->bind_remote_mr(remote_log_mr);  // Bind the log mr as the default remote mr for convenient parameter passing
        log_qps[remote_node.node_id] = log_qp;
        // RDMA_LOG(INFO) << "Thread " << global_tid << ": Log QP connected! with remote node: " << remote_node.node_id << " ip: " << remote_node.ip;
      }
      usleep(2000);
    } while (rc != SUCC);
  }
}