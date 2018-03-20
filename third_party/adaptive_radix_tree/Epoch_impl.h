//
// Created by florian on 22.10.15.
//

#include <assert.h>
#include <iostream>
#include <mutex>
#include <vector>

#include "Epoch.h"

namespace art {

DeletionList::~DeletionList() {
  assert(deletitionListCount == 0 && headDeletionList == nullptr);
  LabelDelete *cur = nullptr, *next = freeLabelDeletes;
  while (next != nullptr) {
    cur = next;
    next = cur->next;
    delete cur;
  }
  freeLabelDeletes = nullptr;
}

std::size_t DeletionList::size() { return deletitionListCount; }

void DeletionList::remove(LabelDelete *label, LabelDelete *prev) {
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

void DeletionList::add(void *n, Deleter deleter_func, uint64_t globalEpoch) {
  deletitionListCount++;
  LabelDelete *label;
  if (headDeletionList != nullptr &&
      headDeletionList->nodesCount < headDeletionList->nodes.size()) {
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
  label->nodes[label->nodesCount] = Garbage{n, deleter_func};
  label->nodesCount++;
  label->epoch = globalEpoch;

  added++;
}

LabelDelete *DeletionList::head() { return headDeletionList; }

void Epoch::enterEpoch(ThreadInfo &threadInfo) {
  unsigned long curEpoch = currentEpoch.load(std::memory_order_relaxed);
  threadInfo.getDeletionList().localEpoch.store(curEpoch,
                                                std::memory_order_release);
}

namespace {
void stdOperatorDelete(void *n) {
  operator delete(n);
}
}  // anonymous namespace

void Epoch::markNodeForDeletion(void *n, ThreadInfo &threadInfo) {
  markNodeForDeletion(n, stdOperatorDelete, threadInfo);
}

void Epoch::markNodeForDeletion(void *n, Deleter deleter_func,
                                ThreadInfo &threadInfo) {
  threadInfo.getDeletionList().add(n, deleter_func, currentEpoch.load());
  threadInfo.getDeletionList().thresholdCounter++;
}

void Epoch::exitEpochAndCleanup(ThreadInfo &threadInfo) {
  DeletionList &deletionList = threadInfo.getDeletionList();
  if ((deletionList.thresholdCounter & (64 - 1)) == 1) {
    currentEpoch++;
  }
  if (deletionList.thresholdCounter > startGCThreshhold) {
    if (deletionList.size() == 0) {
      deletionList.thresholdCounter = 0;
      return;
    }
    deletionList.localEpoch.store(std::numeric_limits<uint64_t>::max());

    uint64_t oldestEpoch = std::numeric_limits<uint64_t>::max();
    {
      auto locked_table = deletionLists.lock_table();
      for (auto &iter : locked_table) {
        auto &dl = iter.second;
        auto e = dl->localEpoch.load();
        if (e < oldestEpoch) {
          oldestEpoch = e;
        }
      }
    }

    LabelDelete *cur = deletionList.head(), *next, *prev = nullptr;
    while (cur != nullptr) {
      next = cur->next;

      if (cur->epoch < oldestEpoch) {
        for (std::size_t i = 0; i < cur->nodesCount; ++i) {
          cur->nodes[i].Delete();
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

Epoch::~Epoch() {
  uint64_t oldestEpoch = std::numeric_limits<uint64_t>::max();
  auto locked_table = deletionLists.lock_table();
  for (auto &iter : locked_table) {
    auto *deletionList = iter.second;
    auto e = deletionList->localEpoch.load();
    if (e < oldestEpoch) {
      oldestEpoch = e;
    }
  }
  for (auto &iter : locked_table) {
    auto *deletionList = iter.second;
    LabelDelete *cur = deletionList->head(), *next, *prev = nullptr;
    while (cur != nullptr) {
      next = cur->next;

      assert(cur->epoch < oldestEpoch);
      for (std::size_t i = 0; i < cur->nodesCount; ++i) {
        cur->nodes[i].Delete();
      }
      deletionList->remove(cur, prev);
      cur = next;
    }
    delete deletionList;
  }
}

void Epoch::showDeleteRatio() {
  auto locked_table = deletionLists.lock_table();
  for (auto &iter : locked_table) {
    auto *dl = iter.second;
    std::cout << "deleted " << dl->deleted << " of " << dl->added << std::endl;
  }
}

DeletionList &Epoch::getDeletionList() {
  DeletionList *threadLocalDeletionList = nullptr;
  auto my_tid = std::this_thread::get_id();
  bool found = deletionLists.find(my_tid, threadLocalDeletionList);
  if (!found) {
    threadLocalDeletionList = new DeletionList();
    deletionLists.insert(my_tid, threadLocalDeletionList);
  }
  return *threadLocalDeletionList;
}

ThreadInfo::ThreadInfo(Epoch &epoch)
    : epoch(epoch), deletionList(epoch.getDeletionList()) {}

DeletionList &ThreadInfo::getDeletionList() const { return deletionList; }

Epoch &ThreadInfo::getEpoch() const { return epoch; }

}  // namespace art