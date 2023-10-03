// Author: Ming Zhang
// Copyright (c) 2022

#include "server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>

#include "util/json_config.h"

int main(int argc, char* argv[]) {
    // Configure of this server
    std::string config_filepath = "../../../config/storage_node_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);

    auto local_node = json_config.get("local_storage_node");
    node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
    node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
    assert(machine_id >= 0 && machine_id < machine_num);
    int local_port = (int)local_node.get("local_port").get_int64();
    int local_meta_port = (int)local_node.get("local_meta_port").get_int64();
    auto log_buf_size_GB = local_node.get("log_buf_size_GB").get_uint64();
    bool use_rdma = (bool)local_node.get("use_rdma").get_bool();

    auto compute_nodes = json_config.get("remote_compute_nodes");
    auto compute_node_ips = compute_nodes.get("compute_node_ips");  // Array
    size_t compute_node_num = compute_node_ips.size();

    // 在这里开始构造disk_manager, log_manager, server
    auto disk_manager = std::make_shared<DiskManager>();
    auto log_manager = std::make_shared<LogManager>();
    auto server = std::make_shared<Server>(local_port, use_rdma, disk_manager.get(), log_manager.get());

}