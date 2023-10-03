#include "storage_rpc.h"

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
        
        log_manager_->add_log_to_buffer(request->log());
        
        return;
    };

    void StoragePoolImpl::GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);

        int fd = request->page_id().fd();
        page_id_t page_no = request->page_id().page_no();

        char data[4096];

        disk_manager_->read_page(fd, page_no, data, 4096);

        response->set_data(std::string(data, 4096));

        return;
    };
}