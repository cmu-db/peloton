//
// Created by Min Huang on 9/20/17.
//

#pragma once

#ifndef PELOTON_EPOCHE_H
#define PELOTON_EPOCHE_H

#include <atomic>
#include <array>
#include "tbb/enumerable_thread_specific.h"
#include "tbb/combinable.h"

namespace peloton {
namespace index {
struct LabelDelete {
  std::array<void*, 32> nodes;
  uint64_t epoche;
  std::size_t nodesCount;
  LabelDelete *next;
};

class DeletionList {
  LabelDelete *headDeletionList = nullptr;
  LabelDelete *freeLabelDeletes = nullptr;
  std::size_t deletitionListCount = 0;

public:
  std::atomic<uint64_t> localEpoche;
  size_t thresholdCounter{0};

  ~DeletionList();
  LabelDelete *head();

  void add(void *n, uint64_t globalEpoch);

  void remove(LabelDelete *label, LabelDelete *prev);

  std::size_t size();

  std::uint64_t deleted = 0;
  std::uint64_t added = 0;
};

class Epoche;
class EpocheGuard;

class ThreadInfo {
  friend class Epoche;
  friend class EpocheGuard;
  Epoche &epoche;
  DeletionList &deletionList;


  DeletionList & getDeletionList() const;
public:

  ThreadInfo(Epoche &epoche);

  ThreadInfo(const ThreadInfo &ti) : epoche(ti.epoche), deletionList(ti.deletionList) {
  }

  ~ThreadInfo();

  Epoche & getEpoche() const;
};

class Epoche {
  friend class ThreadInfo;
  std::atomic<uint64_t> currentEpoche{0};

  tbb::enumerable_thread_specific<DeletionList> deletionLists;

  size_t startGCThreshhold;


public:
  Epoche(size_t startGCThreshhold) : startGCThreshhold(startGCThreshhold) { }

  ~Epoche();

  void enterEpoche(ThreadInfo &epocheInfo);

  void markNodeForDeletion(void *n, ThreadInfo &epocheInfo);

  void exitEpocheAndCleanup(ThreadInfo &info);

  void showDeleteRatio();

};

class EpocheGuard {
  ThreadInfo &threadEpocheInfo;
public:

  EpocheGuard(ThreadInfo &threadEpocheInfo) : threadEpocheInfo(threadEpocheInfo) {
    threadEpocheInfo.getEpoche().enterEpoche(threadEpocheInfo);
  }

  ~EpocheGuard() {
    threadEpocheInfo.getEpoche().exitEpocheAndCleanup(threadEpocheInfo);
  }
};

class EpocheGuardReadonly {
public:

  EpocheGuardReadonly(ThreadInfo &threadEpocheInfo) {
    threadEpocheInfo.getEpoche().enterEpoche(threadEpocheInfo);
  }

  ~EpocheGuardReadonly() {
  }
};

inline ThreadInfo::~ThreadInfo() {
  deletionList.localEpoche.store(std::numeric_limits<uint64_t>::max());
}

}
}

#endif //PELOTON_EPOCHE_H
