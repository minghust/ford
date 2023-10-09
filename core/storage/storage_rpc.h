#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "storage_service.pb.h"
#include "log/log_manager.h"
#include "disk_manager.h"

namespace storage_service{
class StoragePoolImpl : public StorageService{  
  public:
    StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager);

    virtual ~StoragePoolImpl();

    // 计算层向存储层写日志
    virtual void LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done);
    
    // 计算层向存储层读数据页
    virtual void GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done);

  private:
    LogManager* log_manager_;
    DiskManager* disk_manager_;

  };



}