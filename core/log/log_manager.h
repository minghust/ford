#pragma once

#include <string>

#include "base/common.h"
#include "storage/disk_manager.h"
#include "logreplay.h"

class LogManager {
public:
    LogManager(DiskManager* disk_manager, LogReplay* log_replay);
    ~LogManager(){}

    // lsn_t add_log_to_buffer(std::string log_record);
    void write_batch_log_to_disk(std::string batch_log);

    int log_file_fd_;
    DiskManager* disk_manager_;
    LogReplay* log_replay_;
};