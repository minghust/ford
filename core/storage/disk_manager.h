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

using page_id_t = int32_t;   // page id type , 页ID
static constexpr int INVALID_PAGE_ID = -1;                                    // invalid page id

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

template <>
struct std::hash<PageId> {
    size_t operator()(const PageId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};


class DiskManager {
   public:
    explicit DiskManager(){};

    ~DiskManager() = default;

    // TODO
    void read_page(int fd, page_id_t page_no, char *offset, size_t num_bytes){


    }

};