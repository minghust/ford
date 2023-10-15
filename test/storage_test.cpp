#include <pthread.h>
#include <brpc/channel.h>

#include "record/rm_manager.h"
#include "record/rm_file_handle.h"
#include "util/debug.h"
#include "log/log_record.h"
#include "storage/storage_service.pb.h"

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.0.1:12348", "IP address of server");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

/**
 * 整体测试逻辑为generate_log线程调用insert、delete、update等接口生成log，并在完成执行逻辑后把所有的内存页面刷入A disk_manager中
 * 需要测试等代码在接收到log后异步回放log，这里称为B disk_manager，最后比较两个disk_manager中的页面是否相同
 * 比较相同手动比较一下吧，就是比较一下talbe文件和talbe_mem文件是否有区别
*/

brpc::ChannelOptions options;
brpc::Channel channel;

bool need_request_page = false;
page_id_t request_page_no = INVALID_PAGE_ID;
batch_id_t request_batch_id = INVALID_BATCH_ID;

void* request_pages(void* arg) {
    storage_service::StorageService_Stub stub(&channel);

    while(need_request_page == true) {
        RDMA_LOG(INFO) << "begin request page";
        storage_service::GetPageRequest request;
        storage_service::GetPageResponse response;
        brpc::Controller cntl;

        storage_service::GetPageRequest_PageID page_id;
        
        page_id.set_page_no(request_page_no);
        page_id.set_table_name("table");

        request.set_allocated_page_id(&page_id);
        request.set_require_batch_id(request_batch_id);

        stub.GetPage(&cntl, &request, &response, NULL);

        if(!cntl.Failed()) {
            RDMA_LOG(INFO) << "Get page success";
        }

        need_request_page = false;
    }
}

void* generate_redo_logs(void* arg) {
    storage_service::StorageService_Stub stub(&channel);
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
        RDMA_LOG(INFO) << "Write batch1 log succeed, latency = " << cntl.latency_us() << "us";
    }

    request_page_no = rid1.page_no_;
    need_request_page = true;
    buffer_mgr->flush_all_pages(table_file->fd_);
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
    
    request.set_log(txn2->get_log_string());
    stub.LogWrite(&cntl, &request, &response, NULL);

    if(!cntl.Failed()) {
        RDMA_LOG(INFO) << "Write batch2 log succeed, latency = " << cntl.latency_us() << "us";
    }

    request_page_no = rid3.page_no_;
    need_request_page = true;
    RDMA_LOG(INFO) << "try to flush_all_pages";
    buffer_mgr->flush_all_pages(table_file->fd_);
    RDMA_LOG(INFO) << "finish flush_all_pages after executing txn1";

    BatchTxn* txn3 = new BatchTxn(3);
    itemkey_t key4 = 4;
    char* value4 = new char[8];
    std::string name4 = "eeee";
    float score4 = 97.5;
    memcpy(value4, name4.c_str(), name4.length());
    memcpy(value4 + name4.length(), &score4, sizeof(float));
    Rid rid4 = table_file->insert_record(key4, value4, txn3);

    request.set_log(txn3->get_log_string());
    stub.LogWrite(&cntl, &request, &response, NULL);

    if(!cntl.Failed()) {
        RDMA_LOG(INFO) << "Write batch3 log succeed, latency = " << cntl.latency_us() << "us";
    }

    request_page_no = rid4.page_no_;
    need_request_page = true;
    buffer_mgr->flush_all_pages(table_file->fd_);
}

int main(int argc, char* argv[]) {
    options.use_rdma = true;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if(channel.Init(FLAGS_server.c_str(), &options) != 0) {
        RDMA_LOG(FATAL) << "Fail to initialize channel";
    }

    pthread_t log_thread;
    pthread_t page_thread;

    pthread_create(&log_thread, NULL, generate_redo_logs, NULL);
    pthread_create(&page_thread, NULL, request_pages, NULL);

    pthread_join(log_thread, NULL);
    pthread_join(page_thread, NULL);

    return 0;
}