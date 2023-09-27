#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <brpc/channel.h>

#include "storage/storage_rpc.h"
// #include "rlib/rdma_ctrl.hpp"

// Load DB
#include "micro/micro_db.h"
#include "smallbank/smallbank_db.h"
#include "tatp/tatp_db.h"
#include "tpcc/tpcc_db.h"

class Server {
public:
    Server(int local_port, bool use_rdma, DiskManager* disk_manager, LogManager* log_manager): 
        
        disk_manager_(disk_manager), log_manager_(log_manager){
            
            std::thread rpc_thread([&]{
                    
            //启动事务brpc server
            brpc::Server server;

            storage_service::StoragePoolImpl storage_pool_impl(log_manager_, disk_manager_);
            if (server.AddService(&storage_pool_impl, 
                                    brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add service";
            }
            // 监听[0.0.0.0:local_port]
            butil::EndPoint point;
            point = butil::EndPoint(butil::IP_ANY, local_port);

            brpc::ServerOptions options;
            options.use_rdma = use_rdma;

            if (server.Start(point,&options) != 0) {
                LOG(ERROR) << "Fail to start Server";
            }

            server.RunUntilAskedToQuit();
        });

    }

    ~Server() {

    }

private:
    DiskManager* disk_manager_;
    LogManager* log_manager_;
};
