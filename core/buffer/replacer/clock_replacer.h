
#pragma once

#include <mutex>  // NOLINT
#include <vector>

#include "replacer.h"

class ClockReplacer : public Replacer {
    using mutex_t = std::mutex;

   public:
    // EMPTY:     该frame没有存储page或者没有被固定
    // ACCESSED:  该frame不久前被线程使用过
    // UNTOUCHED: 该frame可以被victim获取
    enum class Status { UNTOUCHED, ACCESSED, EMPTY_OR_PINNED };

    /**
     * @description: 创建一个新的ClockReplacer
     * @param {size_t} num_pages ClockReplacer最多需要存储的页的数量
     */
    explicit ClockReplacer(size_t num_pages);

    ~ClockReplacer() override;

    bool victim(frame_id_t* frame_id) override;

    void pin(frame_id_t frame_id) override;

    void unpin(frame_id_t frame_id) override;

    size_t Size() override;

   private:
    std::vector<Status> circular_;
    frame_id_t hand_{0};
    size_t capacity_;
    mutex_t mutex_;
};