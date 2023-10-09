#pragma once

#include <string>

#include "base/common.h"
#include "storage/disk_manager.h"

#define LOG_FILE_NAME "LOG_FILE"

class LogManager {
public:
    LogManager();
    ~LogManager(){}

    // lsn_t add_log_to_buffer(std::string log_record);
    void write_batch_log_to_disk(std::string batch_log);

    int log_file_fd_;
    DiskManager* disk_manager_;    
};