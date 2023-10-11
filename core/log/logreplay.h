#pragma once

#include <thread>
#include <condition_variable>

#include "log_record.h"
#include "storage/disk_manager.h"

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    char buffer_[LOG_REPLAY_BUFFER_SIZE+1];
    int offset_;    // 写入log的offset
};

class LogReplay{
public:
    LogReplay(DiskManager* disk_manager):disk_manager_(disk_manager){
        // 以读写模式打开log_replay文件, log_replay_fd负责顺序读, log_write_head_fd负责写head
        log_replay_fd_ = open(LOG_FILE_NAME, O_RDWR);
        log_write_head_fd_ = open(LOG_FILE_NAME, O_RDWR);

        off_t offset = lseek(log_replay_fd_, 0, SEEK_SET);
        if (offset == -1) {
            std::cerr << "Failed to seek log file." << std::endl;
            assert(0);
        }
        ssize_t bytes_read = read(log_replay_fd_, &persist_batch_id_, sizeof(batch_id_t));
        if(bytes_read != sizeof(batch_id_t)){
            std::cerr << "Failed to read persist_batch_id_." << std::endl;
            assert(0);
        }
        bytes_read = read(log_replay_fd_, &persist_off_, sizeof(uint64_t));
        if(bytes_read != sizeof(uint64_t)){
            std::cerr << "Failed to read persist_off_." << std::endl;
            assert(0);
        }

        max_replay_off_ = disk_manager_->get_file_size(LOG_FILE_NAME) - 1;
        replay_thread_ = std::thread(&LogReplay::replayFun, this);
    };

    ~LogReplay(){
        replay_stop = true;
        if (replay_thread_.joinable()) {
            replay_thread_.join();
        }
        close(log_replay_fd_);
    };

    int  read_log(char *log_data, int size, int offset);
    void apply_sigle_log(LogRecord* log_record);
    void add_max_replay_off_(int off) {max_replay_off_ += off;}
    void replayFun();

private:
    int log_replay_fd_; // 重放log文件fd
    int log_write_head_fd_; // 写文件头fd, 减少lseek次数
    size_t max_replay_off_;
    batch_id_t persist_batch_id_;
    size_t persist_off_;

    DiskManager* disk_manager_;
    LogBuffer buffer_;

    bool replay_stop = false;
    std::thread replay_thread_;
    std::condition_variable cv_; // 条件变量
};