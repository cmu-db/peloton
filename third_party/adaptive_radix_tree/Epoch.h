
#pragma once

#include <atomic>
#include <array>

#include "libcuckoo/cuckoohash_map.hh"

namespace art {

using Deleter = void (*)(void *);

struct Garbage {
  void *n;
  Deleter deleter_func;

  Garbage() : n(nullptr), deleter_func() {}
  Garbage(void *_n, Deleter _deleter_func)
      : n(_n), deleter_func(_deleter_func) {
    assert(n);
    assert(deleter_func);
  }

  // TODO(pmenon): Give this a better name?
  void Delete() {
    assert(n);
    assert(deleter_func);
    deleter_func(n);
  }
};

struct LabelDelete {
  std::array<Garbage, 32> nodes;
  uint64_t epoch;
  std::size_t nodesCount;
  LabelDelete *next;
};

class DeletionList {
  LabelDelete *headDeletionList = nullptr;
  LabelDelete *freeLabelDeletes = nullptr;
  std::size_t deletitionListCount = 0;

 public:
  std::atomic<uint64_t> localEpoch;
  size_t thresholdCounter{0};

  ~DeletionList();

  LabelDelete *head();

  void add(void *n, Deleter deleter_func, uint64_t globalEpoch);

  void remove(LabelDelete *label, LabelDelete *prev);

  std::size_t size();

  std::uint64_t deleted = 0;
  std::uint64_t added = 0;
};

class Epoch;
class EpochGuard;

class ThreadInfo {
  friend class Epoch;
  friend class EpochGuard;
  Epoch &epoch;
  DeletionList &deletionList;

  DeletionList &getDeletionList() const;

 public:
  explicit ThreadInfo(Epoch &epoch);

  ThreadInfo(const ThreadInfo &ti) = default;

  ~ThreadInfo();

  Epoch &getEpoch() const;
};

class Epoch {
  friend class ThreadInfo;
  std::atomic<uint64_t> currentEpoch{0};

  // A container mapping all threads to their garbage
  cuckoohash_map<std::thread::id, DeletionList *> deletionLists;

  size_t startGCThreshhold;

 public:
  Epoch(size_t startGCThreshhold) : startGCThreshhold(startGCThreshhold) {}

  ~Epoch();

  void enterEpoch(ThreadInfo &threadInfo);

  void markNodeForDeletion(void *n, ThreadInfo &threadInfo);
  void markNodeForDeletion(void *n, Deleter deleter_func,
                           ThreadInfo &threadInfo);

  void exitEpochAndCleanup(ThreadInfo &threadInfo);

  void showDeleteRatio();

  DeletionList &getDeletionList();
};

class EpochGuard {
  ThreadInfo &threadEpochInfo;

 public:
  EpochGuard(ThreadInfo &threadEpochInfo) : threadEpochInfo(threadEpochInfo) {
    threadEpochInfo.getEpoch().enterEpoch(threadEpochInfo);
  }

  ~EpochGuard() {
    threadEpochInfo.getEpoch().exitEpochAndCleanup(threadEpochInfo);
  }
};

class EpochGuardReadonly {
 public:
  EpochGuardReadonly(ThreadInfo &threadEpochInfo) {
    threadEpochInfo.getEpoch().enterEpoch(threadEpochInfo);
  }

  ~EpochGuardReadonly() {}
};

inline ThreadInfo::~ThreadInfo() {
  deletionList.localEpoch.store(std::numeric_limits<uint64_t>::max());
}

}  // namespace art
