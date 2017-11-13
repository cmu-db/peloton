//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/art_epoch_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef PELOTON_EPOCHE_H
#define PELOTON_EPOCHE_H

#include <limits>
#include <atomic>
#include <array>
#include "common/logger.h"

namespace peloton {
namespace index {
struct LabelDelete {
  std::array<void *, 32> nodes;
  uint64_t epoche;
  std::size_t nodes_count;
  LabelDelete *next;
};

class DeletionList {
  LabelDelete *head_deletion_list_ = nullptr;
  LabelDelete *free_label_deletes_ = nullptr;
  std::size_t deletion_list_count_ = 0;

 public:
  std::atomic<uint64_t> local_epoch_;
  size_t threshold_counter_{0};
  std::atomic<int> cleanup_latch_{0b00};

  ~DeletionList();
  LabelDelete *head();

  void add(void *n, uint64_t global_epoch);

  void remove(LabelDelete *label, LabelDelete *prev);

  std::size_t size();

  std::uint64_t deleted_ = 0;
  std::uint64_t added_ = 0;
};

class ARTEpochManager;
class EpochGuard;

class ThreadInfo {
  friend class ARTEpochManager;
  friend class EpochGuard;
  ARTEpochManager &epoch_manager_;
  DeletionList deletion_list_;

  DeletionList &GetDeletionList();

 public:
  ThreadInfo(ARTEpochManager &epoch_manager);

  //  ThreadInfo(const ThreadInfo &ti) : epoche(ti.epoche),
  //  deletionList(ti.deletionList) {
  //  }

  ~ThreadInfo();

  ARTEpochManager &GetEpochManager() const;
};

class PaddedThreadInfo {
 public:
  static constexpr size_t ALIGNMENT = 128;
  ThreadInfo thread_info_;

  PaddedThreadInfo(ARTEpochManager &epoch_manager)
      : thread_info_{epoch_manager} {};

 private:
  char padding[ALIGNMENT - sizeof(ThreadInfo)];
};

class ARTEpochManager {
  friend class ThreadInfo;
  std::atomic<uint64_t> current_epoch_{0};

  // This is the presumed size of cache line
  static constexpr size_t CACHE_LINE_SIZE = 128;

  // This is the mask we used for address alignment (AND with this)
  static constexpr size_t CACHE_LINE_MASK = ~(CACHE_LINE_SIZE - 1);

  static constexpr size_t THREAD_NUM = 1024;

  PaddedThreadInfo *thread_info_list_;
  unsigned char *original_pointer_;

  size_t start_GC_threshhold_;

 public:
  ARTEpochManager(size_t start_GC_threshhold_);

  ~ARTEpochManager();

  void EnterEpoch(ThreadInfo &epoch_info);

  void MarkNodeForDeletion(void *n, ThreadInfo &epoch_info);

  void ExitEpochAndCleanup(ThreadInfo &info);

  void ShowDeleteRatio();

  ThreadInfo &getThreadInfoByID(int gc_id);

  std::atomic<uint64_t> thread_info_counter_{0};
};

class EpochGuard {
  ThreadInfo &thread_epoch_info_;

 public:
  EpochGuard(ThreadInfo &thread_epoch_info)
      : thread_epoch_info_(thread_epoch_info) {
    thread_epoch_info_.GetEpochManager().EnterEpoch(thread_epoch_info);
  }

  ~EpochGuard() {
    thread_epoch_info_.GetEpochManager().ExitEpochAndCleanup(
        thread_epoch_info_);
  }
};

class EpochGuardReadonly {
 public:
  EpochGuardReadonly(ThreadInfo &thread_epoch_info) {
    thread_epoch_info.GetEpochManager().EnterEpoch(thread_epoch_info);
  }

  ~EpochGuardReadonly() {}
};

inline ThreadInfo::~ThreadInfo() {
  deletion_list_.local_epoch_.store(std::numeric_limits<uint64_t>::max());
}
}
}

namespace peloton {
namespace index {
inline DeletionList::~DeletionList() {
  assert(deletion_list_count_ == 0 && head_deletion_list_ == nullptr);
  LabelDelete *cur = nullptr, *next = free_label_deletes_;
  while (next != nullptr) {
    cur = next;
    next = cur->next;
    delete cur;
  }
  free_label_deletes_ = nullptr;
}

inline std::size_t DeletionList::size() { return deletion_list_count_; }

inline void DeletionList::remove(LabelDelete *label, LabelDelete *prev) {
  if (prev == nullptr) {
    head_deletion_list_ = label->next;
  } else {
    prev->next = label->next;
  }
  deletion_list_count_ -= label->nodes_count;

  label->next = free_label_deletes_;
  free_label_deletes_ = label;
  deleted_ += label->nodes_count;
}

inline void DeletionList::add(void *n, uint64_t global_epoch) {
  deletion_list_count_++;
  LabelDelete *label;
  if (head_deletion_list_ != nullptr &&
      head_deletion_list_->nodes_count < head_deletion_list_->nodes.size()) {
    label = head_deletion_list_;
  } else {
    if (free_label_deletes_ != nullptr) {
      label = free_label_deletes_;
      free_label_deletes_ = free_label_deletes_->next;
    } else {
      label = new LabelDelete();
    }
    label->nodes_count = 0;
    label->next = head_deletion_list_;
    head_deletion_list_ = label;
  }
  label->nodes[label->nodes_count] = n;
  label->nodes_count++;
  label->epoche = global_epoch;

  added_++;
}

inline LabelDelete *DeletionList::head() { return head_deletion_list_; }

inline void ARTEpochManager::EnterEpoch(ThreadInfo &epoch_info) {
  unsigned long current_epoch = current_epoch_.load(std::memory_order_relaxed);
  epoch_info.GetDeletionList().local_epoch_.store(current_epoch,
                                                  std::memory_order_release);
}

inline void ARTEpochManager::MarkNodeForDeletion(void *n,
                                                 ThreadInfo &epoch_info) {
  epoch_info.GetDeletionList().add(n, current_epoch_.load());
  epoch_info.GetDeletionList().threshold_counter_++;
}

inline void ARTEpochManager::ExitEpochAndCleanup(ThreadInfo &epoch_info) {
  DeletionList &deletion_list = epoch_info.GetDeletionList();
  deletion_list.local_epoch_.store(std::numeric_limits<uint64_t>::max());
  if ((deletion_list.threshold_counter_ & (64 - 1)) == 1) {
    current_epoch_++;
  }
  int latch = deletion_list.cleanup_latch_.load();
  if (deletion_list.threshold_counter_ > start_GC_threshhold_ && latch == 0) {
    if (deletion_list.cleanup_latch_.compare_exchange_strong(latch,
                                                             latch + 1)) {
      // got the clean up latch for this thread gc
      if (deletion_list.size() == 0) {
        deletion_list.threshold_counter_ = 0;
        return;
      }

      uint64_t oldest_epoch = std::numeric_limits<uint64_t>::max();
      for (size_t i = 0; i < thread_info_counter_; i++) {
        auto e = (thread_info_list_ + i)
                     ->thread_info_.GetDeletionList()
                     .local_epoch_.load();
        if (e < oldest_epoch) {
          oldest_epoch = e;
        }
      }

      LabelDelete *cur = deletion_list.head(), *next, *prev = nullptr;
      while (cur != nullptr) {
        next = cur->next;

        if (cur->epoche < oldest_epoch) {
          for (std::size_t i = 0; i < cur->nodes_count; ++i) {
            operator delete(cur->nodes[i]);
          }
          deletion_list.remove(cur, prev);
        } else {
          prev = cur;
        }
        cur = next;
      }
      deletion_list.threshold_counter_ = 0;

      deletion_list.cleanup_latch_.store(0);
    }
  }
}

inline ARTEpochManager::~ARTEpochManager() {
  uint64_t oldest_epoch = std::numeric_limits<uint64_t>::max();
  for (size_t i = 0; i < thread_info_counter_; i++) {
    auto e = (thread_info_list_ + i)
                 ->thread_info_.GetDeletionList()
                 .local_epoch_.load();
    if (e < oldest_epoch) {
      oldest_epoch = e;
    }
  }
  for (size_t i = 0; i < thread_info_counter_; i++) {
    auto &d = (thread_info_list_ + i)->thread_info_.GetDeletionList();
    LabelDelete *cur = d.head(), *next, *prev = nullptr;
    while (cur != nullptr) {
      next = cur->next;
      assert(cur->epoche <= oldest_epoch);
      for (std::size_t i = 0; i < cur->nodes_count; ++i) {
        operator delete(cur->nodes[i]);
      }
      d.remove(cur, prev);
      cur = next;
    }
  }
}

inline void ARTEpochManager::ShowDeleteRatio() {
  for (size_t i = 0; i < thread_info_counter_; i++) {
    auto &d = (thread_info_list_ + i)->thread_info_.GetDeletionList();
    LOG_INFO("deleted %llu of %llu", (unsigned long long)d.deleted_,
             (unsigned long long)d.added_);
  }
}

inline ThreadInfo::ThreadInfo(ARTEpochManager &epoch_manager)
    : epoch_manager_(epoch_manager), deletion_list_() {}

inline DeletionList &ThreadInfo::GetDeletionList() { return deletion_list_; }

inline ARTEpochManager &ThreadInfo::GetEpochManager() const {
  return epoch_manager_;
}
}
}

#endif  // PELOTON_EPOCHE_H
