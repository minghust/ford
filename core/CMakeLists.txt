set(CONNECTION_SRC
        connection/meta_manager.cc
        connection/qp_manager.cc
        )

set(DTX_SRC
        dtx/doorbell.cc
        dtx/dtx_check.cc
        dtx/dtx_check_ro.cc
        dtx/dtx_check_rw.cc
        dtx/dtx_issue.cc
        dtx/dtx.cc
        dtx/dtx_exe_commit.cc
        dtx/dtx_local_meta.cc
        dtx/dtx_compare.cc
        dtx/dtx_compare_issue.cc
        dtx/dtx_compare_check.cc
        )

set(SCHEDULER_SRC
        scheduler/corotine_scheduler.cc
        )

set(STORAGE_SRC
        storage/storage_service.pb.cc
        storage/storage_rpc.cc
        storage/disk_manager.cc
        )

set(BUFFER_SRC
        buffer/bufferpool_manager.cpp
        buffer/replacer/lru_replacer.cpp)

set(RECORD_SRC
        record/rm_file_handle.cpp
        record/rm_manager.cc)
        
set(LOG_SRC
        log/logreplay.cc
        # log/logparse.cc
        log/log_manager.cc)

add_library(ford STATIC
        ${CONNECTION_SRC}
        ${DTX_SRC}
        ${SCHEDULER_SRC}
        ${STORAGE_SRC}
        ${LOG_SRC}
        ${BUFFER_SRC}
        ${RECORD_SRC}
        )

set_target_properties(ford PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(ford rlib pthread boost_coroutine boost_context boost_system)