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
 * 比较相同手动比较一下吧，就是比较一下talbe文件和talbe_mem文件是否有区别
*/

bool need_request_page = false;
page_id_t request_page_no = INVALID_PAGE_ID;

void request_pages(brpc::Channel& channel, storage_service::StorageService_Stub& stub) {
    while(need_request_page == true) {
        storage_service::GetPageRequest request;
        storage_service::GetPageResponse response;
        brpc::Controller cntl;

        storage_service::GetPageRequest_PageID page_id;
        
        storage_service::
        request.set_page_id();

        stub.GetPage(&cntl, &request, &response, NULL);

        if(!cntl.Failed()) {
            RDMA_LOG(INFO) << "Get page";
        }
        need_request_page = false;
    }
}

void* generate_redo_logs(brpc::Channel& channel, storage_service::StorageService_Stub& stub) {
    DiskManager* disk_manager = new DiskManager();
    BufferPoolManager* buffer_mgr = new BufferPoolManager(BUFFER_POOL_SIZE, disk_manager);
    RmManager* rm_manager = new RmManager(disk_manager, buffer_mgr);

    std::string table_name = "table_mem";   // id int(key), name char(4), score float
    rm_manager->create_file(table_name, 8);
    std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(table_name);

    storage_service::LogWriteRequest request;
    storage_service::LogWriteResponse response;
    brpc::Controller cntl;

    BatchTxn* txn1 = new BatchTxn(1);

    itemkey_t key1 = 1;
    char* value1 = new char[8];
    std::string name1 = "aaaa";
    float score1 = 95.5;
    memcpy(value1, name1.c_str(), name1.length());
    memcpy(value1 + name1.length(), &score1, sizeof(float));
    Rid rid1 = table_file->insert_record(key1, value1, txn1);

    itemkey_t key2 = 2;
    char* value2 = new char[8];
    std::string name2 = "bbbb";
    float score2 = 99.5;
    memcpy(value2, name2.c_str(), name2.length());
    memcpy(value2 + name2.length(), &score2, sizeof(float));
    Rid rid2 = table_file->insert_record(key2, value2, txn1);

    request.set_log(txn1->get_log_string());
    stub.LogWrite(&cntl, &request, &response, NULL);

    if(!cntl.Failed()) {
        RDMA_LOG(INFO) << "Write batch log succeed, latency = " << cntl.latency_us() << "us";
    }

    buffer_mgr->flush_all_pages();
    // 写完了txn1的log，并把txn1的修改写入了磁盘，写入了table_mem文件，log_replay的数据写入table文件（log里面的table_name默认为table）

    BatchTxn* txn2 = new BatchTxn(2);
    itemkey_t key3 = 3;
    char* value3 = new char[8];
    std::string name3 = "cccc";
    float score3 = 98.5;
    memcpy(value3, name3.c_str(), name3.length());
    memcpy(value3 + name3.length(), &score3, sizeof(float));
    Rid rid3 = table_file->insert_record(key3, value3, txn2);

    char* new_value2 = new char[8];
    std::string new_name2 = "dddd";
    float new_score2 = 100.0;
    memcpy(new_value2, new_name2.c_str(), new_name2.length());
    memcpy(new_value2 + new_name2.length(), &new_score2, sizeof(float));
    table_file->update_record(rid2, new_value2, txn2);

    table_file->delete_record(rid1, txn2);
    
    request.set_log(txn2->get_log_string);
    stub.LogWrite(&cntl, &request, &response, NULL);
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