// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"

bool DTX::CompareIssueReadRO(std::vector<DirectRead>& pending_direct_ro,
                             std::vector<HashRead>& pending_hash_ro) {
  // Read read-only data from primary
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    auto remote_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    if (USE_LOCAL_ADDR_CACHE) {
      // DrTM+H leverages local address cache
      auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
      if (offset != NOT_FOUND) {
        // Find the addr in local addr cache
        it->remote_offset = offset;
        char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
        pending_direct_ro.emplace_back(DirectRead{.qp = qp, .item = &item, .buf = data_buf, .remote_node = remote_node_id});
        if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
          return false;
        }
        continue;
      }
    }

    // Local cache does not have
    const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
    uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
    offset_t node_off = idx * meta.node_size + meta.base_off;
    char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
    pending_hash_ro.emplace_back(HashRead{.qp = qp, .item = &item, .buf = local_hash_node, .remote_node = remote_node_id, .meta = meta});
    if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off, sizeof(HashNode))) return false;
  }
  return true;
}

bool DTX::CompareIssueReadRW(std::vector<DirectRead>& pending_direct_rw,
                             std::vector<HashRead>& pending_hash_rw,
                             std::vector<InsertOffRead>& pending_insert_off_rw) {
  // Read read-write data
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    not_eager_locked_rw_set.emplace_back(i);
    auto it = read_write_set[i].item_ptr;
    auto remote_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    if (USE_LOCAL_ADDR_CACHE) {
      // DrTM+H leverages local address cache
      auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
      if (offset != NOT_FOUND) {
        it->remote_offset = offset;
        char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
        pending_direct_rw.emplace_back(DirectRead{.qp = qp, .item = &read_write_set[i], .buf = data_buf, .remote_node = remote_node_id});
        if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
          return false;
        }
        continue;
      }
    }

    const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
    uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
    offset_t node_off = idx * meta.node_size + meta.base_off;
    char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
    if (it->user_insert) {
      pending_insert_off_rw.emplace_back(InsertOffRead{.qp = qp, .item = &read_write_set[i], .buf = local_hash_node, .remote_node = remote_node_id, .meta = meta, .node_off = node_off});
    } else {
      pending_hash_rw.emplace_back(HashRead{.qp = qp, .item = &read_write_set[i], .buf = local_hash_node, .remote_node = remote_node_id, .meta = meta});
    }
    if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off, sizeof(HashNode))) return false;
  }
  return true;
}

bool DTX::CompareIssueLocking(std::vector<Lock>& pending_lock) {
  for (auto& index : not_eager_locked_rw_set) {
    locked_rw_set.emplace_back(index);
    char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    *(lock_t*)cas_buf = 0xdeadbeaf;

    auto& it = read_write_set[index].item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    pending_lock.push_back(Lock{.qp = qp, .item = &read_write_set[index], .cas_buf = cas_buf, .lock_off = it->GetRemoteLockAddr()});
    if (!coro_sched->RDMACAS(coro_id, qp, cas_buf, it->GetRemoteLockAddr(), STATE_CLEAN, STATE_LOCKED)) return false;
  }
  return true;
}

bool DTX::CompareIssueValidation(std::vector<Version>& pending_version_read) {
  for (auto& set_it : read_write_set) {
    auto it = set_it.item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
    pending_version_read.push_back(Version{.item = &set_it, .version_buf = version_buf});
    if (!coro_sched->RDMARead(coro_id, qp, version_buf, it->GetRemoteVersionAddr(), sizeof(version_t))) return false;
  }

  for (auto& set_it : read_only_set) {
    auto it = set_it.item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
    pending_version_read.push_back(Version{.item = &set_it, .version_buf = version_buf});
    if (!coro_sched->RDMARead(coro_id, qp, version_buf, it->GetRemoteVersionAddr(), sizeof(version_t))) return false;
  }
  return true;
}

bool DTX::CompareIssueLockValidation(std::vector<ValidateRead>& pending_validate) {
  // For those are not locked during exe phase, we lock and read their versions in a batch
  for (auto& index : not_eager_locked_rw_set) {
    locked_rw_set.emplace_back(index);
    char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    *(lock_t*)cas_buf = 0xdeadbeaf;
    char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
    auto& it = read_write_set[index].item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    pending_validate.push_back(ValidateRead{.qp = qp, .item = &read_write_set[index], .cas_buf = cas_buf, .version_buf = version_buf, .has_lock_in_validate = true});

    std::shared_ptr<LockReadBatch> doorbell = std::make_shared<LockReadBatch>();
    doorbell->SetLockReq(cas_buf, it->GetRemoteLockAddr(), STATE_CLEAN, STATE_LOCKED);
    doorbell->SetReadReq(version_buf, it->GetRemoteVersionAddr(), sizeof(version_t));  // Read a version
    if (!doorbell->SendReqs(coro_sched, qp, coro_id)) {
      return false;
    }
  }
  // For read-only items, we only need to read their versions
  for (auto& set_it : read_only_set) {
    auto it = set_it.item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
    pending_validate.push_back(ValidateRead{.qp = qp, .item = &set_it, .cas_buf = nullptr, .version_buf = version_buf, .has_lock_in_validate = false});
    if (!coro_sched->RDMARead(coro_id, qp, version_buf, it->GetRemoteVersionAddr(), sizeof(version_t))) {
      return false;
    }
  }
  return true;
}

bool DTX::CompareIssueCommitBackup() {
  size_t header_size = sizeof(tx_id) + sizeof(t_id);
  size_t total_size = header_size + DataItemSize;  // header + one data item
  char* log_buffer = (char*)thread_rdma_buffer_alloc->Alloc(total_size);
  char* header = log_buffer;
  // Set log header
  *((tx_id_t*)header) = tx_id;
  header += sizeof(tx_id);
  *((t_id_t*)header) = t_id;
  header += sizeof(t_id);

  for (auto& set_it : read_write_set) {
    auto it = set_it.item_ptr;
    if (!it->user_insert) it->version = tx_id;
    ;  // Maintain the version that user specified
    it->lock = STATE_CLEAN;
    const std::vector<node_id_t>* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
    for (auto node_id : *backup_node_ids) {
      // Use data QP, because writing logs is on the critical path
      RCQP* data_qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
      memcpy(header, (char*)it.get(), DataItemSize);
      offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(node_id, total_size);
      auto& remote_log_mr = thread_qp_man->GetRemoteLogQPWithNodeID(node_id)->remote_mr_;
      auto& local_data_mr = data_qp->local_mr_;
      // We need to use data_qp to write redo logs to backups, because log write is on the critical path.
      // In this way, data_qp will be recorded in the coroutine's pending_counts, and the coro_sched will yield correctly.
      // But we need to write logs, so we have to use log qp to get the remote log region's attr.
      if (!coro_sched->RDMAWrite(coro_id, data_qp, log_buffer, log_offset, total_size, local_data_mr, remote_log_mr)) return false;
    }
  }

  return true;
}

bool DTX::CompareIssueCommitBackupFullFlush() {
  size_t header_size = sizeof(tx_id) + sizeof(t_id);
  size_t total_size = header_size + DataItemSize;  // header + one data item
  char* log_buffer = (char*)thread_rdma_buffer_alloc->Alloc(total_size);
  char* header = log_buffer;
  // Set log header
  *((tx_id_t*)header) = tx_id;
  header += sizeof(tx_id);
  *((t_id_t*)header) = t_id;
  header += sizeof(t_id);

  for (auto& set_it : read_write_set) {
    auto it = set_it.item_ptr;
    if (!it->user_insert) it->version = tx_id;
    const std::vector<node_id_t>* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
    for (auto node_id : *backup_node_ids) {
      // Use data QP, because writing logs is on the critical path
      RCQP* data_qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
      memcpy(header, (char*)it.get(), DataItemSize);
      offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(node_id, total_size);
      auto& remote_log_mr = thread_qp_man->GetRemoteLogQPWithNodeID(node_id)->remote_mr_;
      auto& local_data_mr = data_qp->local_mr_;
      // We need to use data_qp to write redo logs to backups, because log write is on the critical path.
      // In this way, data_qp will be recorded in the coroutine's pending_counts, and the coro_sched will yield correctly.
      // But we need to write logs, so we have to use log qp to get the remote log region's attr.
      if (!coro_sched->RDMAWrite(coro_id, data_qp, log_buffer, log_offset, total_size, local_data_mr, remote_log_mr)) return false;
      // RDMA FLUSH
      char* flush_buffer = (char*)thread_rdma_buffer_alloc->Alloc(RFlushReadSize);
      if (!coro_sched->RDMARead(coro_id, data_qp, flush_buffer, log_offset, RFlushReadSize)) return false;
    }
  }
  return true;
}

bool DTX::CompareIssueCommitBackupSelectiveFlush() {
  size_t header_size = sizeof(tx_id) + sizeof(t_id);
  size_t total_size = header_size + DataItemSize;  // header + one data item

  ssize_t current_i = 0;
  for (auto& set_it : read_write_set) {
    // Each read-write data needs to identify which machine is its backup machine
    auto it = set_it.item_ptr;
    if (!it->user_insert) it->version = tx_id;
    const std::vector<node_id_t>* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
    for (auto node_id : *backup_node_ids) {
      char* log_buffer = (char*)thread_rdma_buffer_alloc->Alloc(total_size);
      char* header = log_buffer;
      memcpy(header, &tx_id, sizeof(tx_id));
      header += sizeof(tx_id);
      memcpy(header, &t_id, sizeof(t_id));
      header += sizeof(t_id);
      memcpy(header, (char*)it.get(), DataItemSize);
      // Use data QP, because writing logs is on the critical path
      RCQP* data_qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
      offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(node_id, total_size);
      auto& remote_log_mr = thread_qp_man->GetRemoteLogQPWithNodeID(node_id)->remote_mr_;
      auto& local_data_mr = data_qp->local_mr_;
      // We need to use data_qp to write redo logs to backups, because log write is on the critical path.
      // In this way, data_qp will be recorded in the coroutine's pending_counts, and the coro_sched will yield correctly.
      // Since we need to write logs, we have to use log qp to get the remote log region's attr.
      if (!coro_sched->RDMAWrite(coro_id, data_qp, log_buffer, log_offset, total_size, local_data_mr, remote_log_mr)) return false;

      if (current_i == read_write_set.size() - 1) {
        // Selective Remote FLUSH
        char* flush_buffer = (char*)thread_rdma_buffer_alloc->Alloc(RFlushReadSize);
        if (!coro_sched->RDMARead(coro_id, data_qp, flush_buffer, log_offset, RFlushReadSize)) return false;
      }
    }

    current_i++;
  }
  return true;
}

bool DTX::CompareIssueCommitBackupBatchSelectFlush() {
  size_t header_size = sizeof(tx_id) + sizeof(t_id);
  size_t total_size = header_size + DataItemSize;  // header + one data item
  char* log_buffer = (char*)thread_rdma_buffer_alloc->Alloc(total_size);
  char* flush_buffer = (char*)thread_rdma_buffer_alloc->Alloc(RFlushReadSize);
  char* header = log_buffer;
  // Set log header
  *((tx_id_t*)header) = tx_id;
  header += sizeof(tx_id);
  *((t_id_t*)header) = t_id;
  header += sizeof(t_id);

  ssize_t current_i = 0;
  for (auto& set_it : read_write_set) {
    auto it = set_it.item_ptr;
    if (!it->user_insert) it->version = tx_id;
    const std::vector<node_id_t>* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
    for (auto node_id : *backup_node_ids) {
      // Use data QP, because writing logs is on the critical path
      RCQP* data_qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
      memcpy(header, (char*)it.get(), DataItemSize);
      offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(node_id, total_size);
      auto& remote_log_mr = thread_qp_man->GetRemoteLogQPWithNodeID(node_id)->remote_mr_;
      auto& local_data_mr = data_qp->local_mr_;
      // We need to use data_qp to write redo logs to backups, because log write is on the critical path.
      // In this way, data_qp will be recorded in the coroutine's pending_counts, and the coro_sched will yield correctly.
      // But we need to write logs, so we have to use log qp to get the remote log region's attr.

      if (current_i == read_write_set.size() - 1) {
        // Batched selective Remote FLUSH
        std::shared_ptr<WriteFlushBatch> doorbell = std::make_shared<WriteFlushBatch>();

        doorbell->SetWriteRemoteReq(log_buffer, log_offset, total_size);
        doorbell->SetReadRemoteReq(flush_buffer, log_offset, RFlushReadSize);
        if (!doorbell->SendReqs(coro_sched, data_qp, coro_id, remote_log_mr)) return false;
      } else {
        if (!coro_sched->RDMAWrite(coro_id, data_qp, log_buffer, log_offset, total_size, local_data_mr, remote_log_mr)) return false;
      }
    }

    current_i++;
  }
  return true;
}

bool DTX::CompareIssueCommitPrimary() {
  for (auto& set_it : read_write_set) {
    auto it = set_it.item_ptr;

    node_id_t node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

    memcpy(data_buf, (char*)it.get(), DataItemSize);

    it->lock = STATE_LOCKED;  // version has been added during commit backup

    std::shared_ptr<WriteUnlockBatch> doorbell = std::make_shared<WriteUnlockBatch>();
    doorbell->SetWritePrimaryReq(data_buf, it->remote_offset, DataItemSize);


    *(lock_t*)cas_buf = STATE_CLEAN;
    doorbell->SetUnLockReq(cas_buf, it->GetRemoteLockAddr());
    if (!doorbell->SendReqs(coro_sched, qp, coro_id, 0)) return false;

#if RFLUSH == 1
    // RDMA full FLUSH. Selective flush does not open this
    char* flush_buffer = (char*)thread_rdma_buffer_alloc->Alloc(RFlushReadSize);
    if (!coro_sched->RDMARead(coro_id, qp, flush_buffer, it->remote_offset, RFlushReadSize)) return false;
#endif
  }
  return true;
}

bool DTX::CompareIssueTruncate() {
  for (auto& set_it : read_write_set) {
    auto it = set_it.item_ptr;
    // In-place update the backups
    it->lock = STATE_CLEAN;
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    memcpy(data_buf, (char*)it.get(), DataItemSize);
    const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
    auto offset_in_backup_hash_store = it->remote_offset - primary_hash_meta.base_off;

    // Get all the backup queue pairs and hash metas for this table
    auto* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
    if (!backup_node_ids) continue;  // There are no backups in the PM pool
    const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(it->table_id);
    // backup_node_ids guarantees that the order of remote machine is the same in backup_hash_metas and backup_qps
    for (size_t i = 0; i < backup_node_ids->size(); i++) {
      auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
      char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
      it->remote_offset = remote_item_off;
      memcpy(data_buf, (char*)it.get(), DataItemSize);
      RCQP* backup_qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));
      auto rc = coro_sched->RDMAWrite(coro_id, backup_qp, data_buf, remote_item_off, DataItemSize);
      if (rc != true) {
        RDMA_LOG(INFO) << "Thread " << t_id << " , Coroutine " << coro_id << " truncate fails";
        return false;
      }
    }
  }

  return true;
}