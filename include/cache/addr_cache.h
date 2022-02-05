// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <map>
#include <unordered_map>

#include "common/common.h"

const offset_t NOT_FOUND = -1;

// For fast remote address lookup
class AddrCache {
 public:
  void Insert(node_id_t remote_node_id, table_id_t table_id, itemkey_t key, offset_t remote_offset) {
    auto node_search = addr_map.find(remote_node_id);
    if (node_search == addr_map.end()) {
      // There is no such node. Init the node and table
      addr_map[remote_node_id] = std::unordered_map<table_id_t, std::unordered_map<itemkey_t, offset_t>>();
      addr_map[remote_node_id][table_id] = std::unordered_map<itemkey_t, offset_t>();
    } else if (node_search->second.find(table_id) == node_search->second.end()) {
      // The node exists, but the table does not exist. Init the table
      addr_map[remote_node_id][table_id] = std::unordered_map<itemkey_t, offset_t>();
    }

    // The node and table both exist, then insert/update the <key,offset> pair
    addr_map[remote_node_id][table_id][key] = remote_offset;
  }

  // We know which node to read, but we do not konw whether it is cached before
  offset_t Search(node_id_t remote_node_id, table_id_t table_id, itemkey_t key) {
    auto node_search = addr_map.find(remote_node_id);
    if (node_search == addr_map.end()) return NOT_FOUND;
    auto table_search = node_search->second.find(table_id);
    if (table_search == node_search->second.end()) return NOT_FOUND;
    auto offset_search = table_search->second.find(key);
    return offset_search == table_search->second.end() ? NOT_FOUND : offset_search->second;
  }

  // If we have read this record, we do not read it from another node
  void Search(table_id_t query_table_id, itemkey_t query_key, node_id_t& remote_node_id, offset_t& remote_offset) {
    // look up node first
    for (auto it = addr_map.begin(); it != addr_map.end(); it++) {
      auto table_search = it->second.find(query_table_id);
      if (table_search == it->second.end()) {
        continue;
      }

      auto offset_search = table_search->second.find(query_key);
      if (offset_search == table_search->second.end()) {
        // No such key. Change to hash read
        return;
      }

      // Tableid and key match. Get the cached remote node id and remote offset
      remote_node_id = it->first;
      remote_offset = offset_search->second;
      return;
    }
  }

  size_t TotalAddrSize() {
    size_t total_size = 0;
    for (auto it = addr_map.begin(); it != addr_map.end(); it++) {
      total_size += sizeof(node_id_t);
      for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
        total_size += sizeof(table_id_t);
        for (auto it3 = it2->second.begin(); it3 != it2->second.end(); it3++) {
          total_size += (sizeof(itemkey_t) + sizeof(offset_t));
        }
      }
    }

    return total_size;
  }

 private:
  std::unordered_map<node_id_t, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, offset_t>>> addr_map;
};