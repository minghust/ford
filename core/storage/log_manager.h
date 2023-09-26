#pragma once

#include <mutex>
#include <vector>
#include <iostream>
#include <thread>
#include <string.h>

using txn_id_t = int32_t;    // transaction id type
using lsn_t = int32_t;       // log sequence number type
// the offset of log_type_ in log header
static constexpr int OFFSET_LOG_TYPE = 0;
// the offset of lsn_ in log header
static constexpr int OFFSET_LSN = sizeof(int);
// the offset of log_tot_len_ in log header
static constexpr int OFFSET_LOG_TOT_LEN = OFFSET_LSN + sizeof(lsn_t);
// the offset of log_tid_ in log header
static constexpr int OFFSET_LOG_TID = OFFSET_LOG_TOT_LEN + sizeof(uint32_t);
// the offset of prev_lsn_ in log header
static constexpr int OFFSET_PREV_LSN = OFFSET_LOG_TID + sizeof(txn_id_t);
// offset of log data
static constexpr int OFFSET_LOG_DATA = OFFSET_PREV_LSN + sizeof(lsn_t);
// sizeof log_header
static constexpr int LOG_HEADER_SIZE = OFFSET_LOG_DATA;


/* 日志记录对应操作的类型 */
enum LogType: int {
    UPDATE = 0,
    INSERT,
    DELETE,
    begin,
    commit,
    ABORT
};
static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT"
};

class LogRecord {
public:
    LogType log_type_;         /* 日志对应操作的类型 */
    lsn_t lsn_;                /* 当前日志的lsn */
    uint32_t log_tot_len_;     /* 整个日志记录的长度 */
    txn_id_t log_tid_;         /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;           /* 事务创建的前一条日志记录的lsn，用于undo */

    // 把日志记录序列化到dest中
    virtual void serialize (char* dest) const {
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }
    // 从src中反序列化出一条日志记录
    virtual void deserialize(const char* src) {
        log_type_ = *reinterpret_cast<const LogType*>(src);
        lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_LSN);
        log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = *reinterpret_cast<const txn_id_t*>(src + OFFSET_LOG_TID);
        prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_PREV_LSN);
    }
    // used for debug
    virtual void format_print() {
        std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
        printf("lsn: %d\n", lsn_);
        printf("log_tot_len: %d\n", log_tot_len_);
        printf("log_tid: %d\n", log_tid_);
        printf("prev_lsn: %d\n", prev_lsn_);
    }

    virtual ~LogRecord() {}
};

class LogManager
{
private:
public:
    LogManager(){};
    ~LogManager(){};

    // TODO 在这里把log加入buffer, 然后异步重放日志
    lsn_t add_log_to_buffer(std::string log_record){
        
    };
};

