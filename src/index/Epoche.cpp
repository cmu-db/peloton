//
// Created by Min Huang on 9/20/17.
//

#include <assert.h>
#include <iostream>
#include "index/Epoche.h"

namespace peloton {
namespace index {
inline DeletionList::~DeletionList() {
  assert(deletitionListCount == 0 && headDeletionList == nullptr);
  LabelDelete *cur = nullptr, *next = freeLabelDeletes;
  while (next != nullptr) {
    cur = next;
    next = cur->next;
    delete cur;
  }
  freeLabelDeletes = nullptr;
}

inline std::size_t DeletionList::size() {
  return deletitionListCount;
}

inline void DeletionList::remove(LabelDelete *label, LabelDelete *prev) {
  if (prev == nullptr) {
    headDeletionList = label->next;
  } else {
    prev->next = label->next;
  }
  deletitionListCount -= label->nodesCount;

  label->next = freeLabelDeletes;
  freeLabelDeletes = label;
  deleted += label->nodesCount;
}

inline void DeletionList::add(void *n, uint64_t globalEpoch) {
  deletitionListCount++;
  LabelDelete *label;
  if (headDeletionList != nullptr && headDeletionList->nodesCount < headDeletionList->nodes.size()) {
    label = headDeletionList;
  } else {
    if (freeLabelDeletes != nullptr) {
      label = freeLabelDeletes;
      freeLabelDeletes = freeLabelDeletes->next;
    } else {
      label = new LabelDelete();
    }
    label->nodesCount = 0;
    label->next = headDeletionList;
    headDeletionList = label;
  }
  label->nodes[label->nodesCount] = n;
  label->nodesCount++;
  label->epoche = globalEpoch;

  added++;
}

inline LabelDelete *DeletionList::head() {
  return headDeletionList;
}

inline void Epoche::enterEpoche(ThreadInfo &epocheInfo) {
  unsigned long curEpoche = currentEpoche.load(std::memory_order_relaxed);
  epocheInfo.getDeletionList().localEpoche.store(curEpoche, std::memory_order_release);
}

inline void Epoche::markNodeForDeletion(void *n, ThreadInfo &epocheInfo) {
  epocheInfo.getDeletionList().add(n, currentEpoche.load());
  epocheInfo.getDeletionList().thresholdCounter++;
}

inline void Epoche::exitEpocheAndCleanup(ThreadInfo &epocheInfo) {
  DeletionList &deletionList = epocheInfo.getDeletionList();
  if ((deletionList.thresholdCounter & (64 - 1)) == 1) {
    currentEpoche++;
  }
  if (deletionList.thresholdCounter > startGCThreshhold) {
    if (deletionList.size() == 0) {
      deletionList.thresholdCounter = 0;
      return;
    }
    deletionList.localEpoche.store(std::numeric_limits<uint64_t>::max());

    uint64_t oldestEpoche = std::numeric_limits<uint64_t>::max();
    for (auto &epoche : deletionLists) {
      auto e = epoche.localEpoche.load();
      if (e < oldestEpoche) {
        oldestEpoche = e;
      }
    }

    LabelDelete *cur = deletionList.head(), *next, *prev = nullptr;
    while (cur != nullptr) {
      next = cur->next;

      if (cur->epoche < oldestEpoche) {
        for (std::size_t i = 0; i < cur->nodesCount; ++i) {
          operator delete(cur->nodes[i]);
        }
        deletionList.remove(cur, prev);
      } else {
        prev = cur;
      }
      cur = next;
    }
    deletionList.thresholdCounter = 0;
  }
}

inline Epoche::~Epoche() {
  uint64_t oldestEpoche = std::numeric_limits<uint64_t>::max();
  for (auto &epoche : deletionLists) {
    auto e = epoche.localEpoche.load();
    if (e < oldestEpoche) {
      oldestEpoche = e;
    }
  }
  for (auto &d : deletionLists) {
    LabelDelete *cur = d.head(), *next, *prev = nullptr;
    while (cur != nullptr) {
      next = cur->next;

      assert(cur->epoche < oldestEpoche);
      for (std::size_t i = 0; i < cur->nodesCount; ++i) {
        operator delete(cur->nodes[i]);
      }
      d.remove(cur, prev);
      cur = next;
    }
  }
}

inline void Epoche::showDeleteRatio() {
  for (auto &d : deletionLists) {
    std::cout << "deleted " << d.deleted << " of " << d.added << std::endl;
  }
}

inline ThreadInfo::ThreadInfo(Epoche &epoche)
  : epoche(epoche), deletionList(epoche.deletionLists.local()) { }

inline DeletionList &ThreadInfo::getDeletionList() const {
  return deletionList;
}

inline Epoche &ThreadInfo::getEpoche() const {
  return epoche;
}

}
}