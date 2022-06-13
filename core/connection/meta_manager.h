// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <atomic>
#include <unordered_map>

#include "base/common.h"
#include "memstore/hash_store.h"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

// const size_t LOG_BUFFER_SIZE = 1024 * 1024 * 512;

struct RemoteNode {
  node_id_t node_id;
  std::string ip;
  int port;
};

class MetaManager {
 public:
  MetaManager();

  node_id_t GetMemStoreMeta(std::string& remote_ip, int remote_port);

  void GetMRMeta(const RemoteNode& node);

  /*** Memory Store Metadata ***/
  ALWAYS_INLINE
  const HashMeta& GetPrimaryHashMetaWithTableID(const table_id_t table_id) const {
    auto search = primary_hash_metas.find(table_id);
    assert(search != primary_hash_metas.end());
    return search->second;
  }

  ALWAYS_INLINE
  const std::vector<HashMeta>* GetBackupHashMetasWithTableID(const table_id_t table_id) const {
    // if (backup_hash_metas.empty()) {
    //   return nullptr;
    // }
    // auto search = backup_hash_metas.find(table_id);
    // assert(search != backup_hash_metas.end());
    // return &(search->second);
    return &(backup_hash_metas[table_id]);
  }

  /*** Node ID Metadata ***/
  ALWAYS_INLINE
  node_id_t GetPrimaryNodeID(const table_id_t table_id) const {
    auto search = primary_table_nodes.find(table_id);
    assert(search != primary_table_nodes.end());
    return search->second;
  }

  ALWAYS_INLINE
  const std::vector<node_id_t>* GetBackupNodeID(const table_id_t table_id) {
    // if (backup_table_nodes.empty()) {
    //   return nullptr;
    // }
    // auto search = backup_table_nodes.find(table_id);
    // assert(search != backup_table_nodes.end());
    // return &(search->second);
    return &(backup_table_nodes[table_id]);
  }

  ALWAYS_INLINE
  const MemoryAttr& GetRemoteLogMR(const node_id_t node_id) const {
    auto mrsearch = remote_log_mrs.find(node_id);
    assert(mrsearch != remote_log_mrs.end());
    return mrsearch->second;
  }

  /*** RDMA Memory Region Metadata ***/
  ALWAYS_INLINE
  const MemoryAttr& GetRemoteHashMR(const node_id_t node_id) const {
    auto mrsearch = remote_hash_mrs.find(node_id);
    assert(mrsearch != remote_hash_mrs.end());
    return mrsearch->second;
  }

 private:
  std::unordered_map<table_id_t, HashMeta> primary_hash_metas;

  // std::unordered_map<table_id_t, std::vector<HashMeta>> backup_hash_metas;

  std::vector<HashMeta> backup_hash_metas[MAX_DB_TABLE_NUM];

  std::unordered_map<table_id_t, node_id_t> primary_table_nodes;

  // std::unordered_map<table_id_t, std::vector<node_id_t>> backup_table_nodes;

  std::vector<node_id_t> backup_table_nodes[MAX_DB_TABLE_NUM];

  std::unordered_map<node_id_t, MemoryAttr> remote_hash_mrs;

  std::unordered_map<node_id_t, MemoryAttr> remote_log_mrs;

  node_id_t local_machine_id;

 public:
  // Used by QP manager and RDMA Region
  RdmaCtrlPtr global_rdma_ctrl;

  std::vector<RemoteNode> remote_nodes;

  RNicHandler* opened_rnic;

  // Below are some parameteres from json file
  int64_t txn_system;
};
