#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "log_manager.h"

LogManager::LogManager(DiskManager* disk_manager, LogReplay* log_replay)
        :disk_manager_(disk_manager), log_replay_(log_replay) {
    log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);
}

void LogManager::write_batch_log_to_disk(std::string batch_log) {
    if (log_file_fd_ == -1) {
        log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);
    }

    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log.c_str(), batch_log.length() * sizeof(char));
    assert(bytes_write != batch_log.length() * sizeof(char));

    // // write persist batch id
    // lseek(log_file_fd_, 0, SEEK_SET);
    // bytes_write = write(log_file_fd_, &log_replay_->persist_batch_id_, sizeof(int));
    // assert(bytes_write != sizeof(int));
}