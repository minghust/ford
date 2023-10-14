#include <pthread.h>
#include <brpc/channel.h>

#include "core/record/rm_manager.h"
#include "core/record/rm_file_handle.h"
#include "core/util/debug.h"
#include "core/log/log_record.h"
#include "core/storage/storage_service.pb.h"

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8000", "IP address of server");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

/**
 * 整体测试逻辑为generate_log线程调用insert、delete、update等接口生成log，并在完成执行逻辑后把所有的内存页面刷入A disk_manager中
 * 需要测试等代码在接收到log后异步回放log，这里称为B disk_manager，最后比较两个disk_manager中的页面是否相同
*/

void request_pages(brpc::Channel& channel, storage_service::StorageService_Stub& stub) {
    
    storage_service::GetPageRequest request;
    storage_service::GetPageResponse response;
    brpc::Controller cntl;

    request.set_page_id();

    stub.GetPage(&cntl, &request, &response, NULL);

    if(!cntl.Failed()) {
        RDMA_LOG(INFO) << "Get page";
    }
    usleep(FLAGS_interval_ms * 10 * 1000L);
}

void* generate_redo_logs(brpc::Channel& channel, storage_service::StorageService_Stub& stub) {
    for(int batch_id = 1; batch_id < 3; ++batch_id) {
        storage_service::LogWriteRequest request;
        storage_service::LogWriteResponse response;
        brpc::Controller cntl;

        BatchTxn* txn = new BatchTxn(batch_id);

        request.set_log();

        stub.LogWrite(&cntl, &request, &response, NULL);

        if(!cntl.Failed()) {
            RDMA_LOG(INFO) << "Write batch log succeed, latency = " << cntl.latency_us() << "us";
        }
    }
}

int main(int argc, char* argv[]) {
    brpc::ChannelOptions options;
    brpc::Channel channel;
    
    options.use_rdma = true;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if(channel.Init(FLAGS_server.c_str(), &options) != 0) {
        RDMA_LOG(FATAL) << "Fail to initialize channel";
    }

    storage_service::StorageService_Stub stub(&channel);

    pthread_t log_thread;
    pthread_t page_thread;

    pthread_create(&log_thread, NULL, generate_redo_logs, channel, stub);
    pthread_create(&page_thread, NULL, request_pages, channel, stub);

    pthread_join(log_thread);
    pthread_join(page_thread);

    return 0;
}