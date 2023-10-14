#pragma once

#include <deque>

#include "base/common.h"
#include "log/log_record.h"

class BatchTxn {
public:
    BatchTxn(){}
    BatchTxn(batch_id_t batch_id) {
        batch_id_ = batch_id;
    }
    batch_id_t batch_id_;

    std::deque<LogRecord*> logs;

};