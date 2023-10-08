#pragma once

#include "log_record.h"
#include "storage/disk_manager.h"

class LogReplay{
public:
    void apply_sigle_log(LogRecord* log_record);

    batch_id_t persist_batch_id_;
    DiskManager* disk_manager_;
};