#pragma once

#include <fcntl.h>     
#include <sys/stat.h>  
#include <unistd.h>    
#include <atomic>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>

#include "base/common.h"

/**
 * @description: 存储层每个Page的id的声明
 */
struct PageId {
    int fd;  //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    page_id_t page_no = INVALID_PAGE_ID;

    friend bool operator==(const PageId &x, const PageId &y) { return x.fd == y.fd && x.page_no == y.page_no; }
    bool operator<(const PageId& x) const {
        if(fd < x.fd) return true;
        return page_no < x.page_no;
    }

    std::string toString() {
        return "{fd: " + std::to_string(fd) + " page_no: " + std::to_string(page_no) + "}"; 
    }

    inline int64_t Get() const {
        return (static_cast<int64_t>(fd << 16) | page_no);
    }
};

// PageId的自定义哈希算法, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
struct PageIdHash {
    size_t operator()(const PageId &x) const { return (x.fd << 16) | x.page_no; }
};

// template <>
// struct std::hash<PageId> {
//     size_t operator()(const PageId &obj) const { return std::hash<int64_t>()(obj.Get()); }
// };


class DiskManager {
   public:
    explicit DiskManager();

    ~DiskManager() = default;

    void write_page(int fd, page_id_t page_no, const char *offset, int num_bytes);

    void read_page(int fd, page_id_t page_no, char *offset, int num_bytes);

    // update part of page data, value_size: num_bytes
    void update_value(int fd, page_id_t page_no, int slot_offset, char* value, int value_size);

    // void set_bitmap(int fd, page_id_t page_no, int pos);

    // void reset_bitmap(int fd, page_id_t page_no, int pos);

    page_id_t allocate_page(int fd);

    void deallocate_page(page_id_t page_id);

    /*目录操作*/
    bool is_dir(const std::string &path);

    void create_dir(const std::string &path);

    void destroy_dir(const std::string &path);

    /*文件操作*/
    bool is_file(const std::string &path);

    void create_file(const std::string &path);

    void destroy_file(const std::string &path);

    int open_file(const std::string &path);

    void close_file(int fd);

    int get_file_size(const std::string &file_name);

    std::string get_file_name(int fd);

    int get_file_fd(const std::string &file_name);

    int read_log(char *log_data, int size, int offset);
    
    void write_log(char *log_data, int size);

    void SetLogFd(int log_fd) { log_fd_ = log_fd; }

    int GetLogFd() { return log_fd_; }

    /**
     * @description: 设置文件已经分配的页面个数
     * @param {int} fd 文件对应的文件句柄
     * @param {int} start_page_no 已经分配的页面个数，即文件接下来从start_page_no开始分配页面编号
     */
    void set_fd2pageno(int fd, int start_page_no) { fd2pageno_[fd] = start_page_no; }

    /**
     * @description: 获得文件目前已分配的页面个数，即如果文件要分配一个新页面，需要从fd2pagenp_[fd]开始分配
     * @return {page_id_t} 已分配的页面个数 
     * @param {int} fd 文件对应的句柄
     */
    page_id_t get_fd2pageno(int fd) { return fd2pageno_[fd]; }

    static constexpr int MAX_FD = 8192;

   private:
    // 文件打开列表，用于记录文件是否被打开
    std::unordered_map<std::string, int> path2fd_;  //<Page文件磁盘路径,Page fd>哈希表
    std::unordered_map<int, std::string> fd2path_;  //<Page fd,Page文件磁盘路径>哈希表

    int log_fd_ = -1;                             // WAL日志文件的文件句柄，默认为-1，代表未打开日志文件
    std::atomic<page_id_t> fd2pageno_[MAX_FD]{};  // 文件中已经分配的页面个数，初始值为0
};