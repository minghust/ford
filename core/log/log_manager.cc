#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "log_manager.h"

// lsn_t LogManager::add_log_to_buffer(std::string log_record) {
    
// }

LogManager::LogManager() {
    log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);
}

void LogManager::write_batch_log_to_disk(std::string batch_log) {
    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log.c_str(), batch_log.length() * sizeof(char));
    assert(bytes_write != batch_log.length() * sizeof(char));
}