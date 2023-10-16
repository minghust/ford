#include "storage_rpc.h"
#include "util/debug.h"

namespace storage_service{

    StoragePoolImpl::StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager)
        :log_manager_(log_manager), disk_manager_(disk_manager){};

    StoragePoolImpl::~StoragePoolImpl(){};

    // 计算层向存储层写日志
    void StoragePoolImpl::LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done){
            
        brpc::ClosureGuard done_guard(done);
        
        log_manager_->write_batch_log_to_disk(request->log());
        
        return;
    };

    void StoragePoolImpl::GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);

        RDMA_LOG(INFO) << "handle GetPage request";

        std::string fd = request->page_id().table_name();
        page_id_t page_no = request->page_id().page_no();

        char data[4096];

        // TODO
        disk_manager_->read_page(1, page_no, data, 4096);

        response->set_data(std::string(data, 4096));

        return;
    };
}