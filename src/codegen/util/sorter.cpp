//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.cpp
//
// Identification: src/codegen/util/sorter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/sorter.h"

#include <queue>

#include "pdqsort/pdqsort.h"

#include "common/synchronization/count_down_latch.h"
#include "common/timer.h"
#include "storage/backend_manager.h"
#include "threadpool/mono_queue_pool.h"

namespace peloton {
namespace codegen {
namespace util {

Sorter::Sorter(ComparisonFunction func, uint32_t tuple_size)
    : cmp_func_(func),
      tuple_size_(tuple_size),
      buffer_pos_(nullptr),
      buffer_end_(nullptr),
      next_alloc_size_(kInitialBufferSize),
      tuples_start_(nullptr),
      tuples_end_(nullptr) {
  // No memory allocation
  LOG_DEBUG("Initialized Sorter for tuples of size %u bytes", tuple_size_);
}

Sorter::~Sorter() {
  uint64_t total_alloc = 0;
  auto &backend_manager = storage::BackendManager::GetInstance();
  for (const auto &iter : blocks_) {
    void *block = iter.first;
    total_alloc += iter.second;
    PELOTON_ASSERT(block != nullptr);
    backend_manager.Release(BackendType::MM, block);
  }
  buffer_pos_ = buffer_end_ = nullptr;
  tuples_start_ = tuples_end_ = nullptr;
  next_alloc_size_ = 0;

  LOG_DEBUG("Cleaned up %zu tuples from %zu blocks of memory (%.2lf KB)",
            tuples_.size(), blocks_.size(), total_alloc / 1024.0);
}

void Sorter::Init(Sorter &sorter, ComparisonFunction func,
                  uint32_t tuple_size) {
  new (&sorter) Sorter(func, tuple_size);
}

void Sorter::Destroy(Sorter &sorter) { sorter.~Sorter(); }

char *Sorter::StoreInputTuple() {
  // Make room for a new tuple
  MakeRoomForNewTuple();

  // Bump the position pointer, return location where call can write a tuple
  char *ret = buffer_pos_;
  buffer_pos_ += tuple_size_;

  // Track tuple position
  tuples_.push_back(ret);

  // Finish
  return ret;
}

void Sorter::Sort() {
  // Short-circuit
  if (tuples_.empty()) {
    return;
  }

  // Time it
  Timer<std::milli> timer;
  timer.Start();

  // Sort the sucker
  auto cmp = [this](char *l, char *r) { return cmp_func_(l, r) < 0; };
  pdqsort(tuples_.begin(), tuples_.end(), cmp);

  // Setup pointers
  tuples_start_ = tuples_.data();
  tuples_end_ = tuples_start_ + tuples_.size();

  timer.Stop();

  LOG_DEBUG("Sorted %zu tuples in %.2f ms", tuples_.size(),
            timer.GetDuration());
}

void Sorter::SortParallel(
    const executor::ExecutorContext::ThreadStates &thread_states,
    uint32_t sorter_offset) {
  // Pull out the sorter instances
  uint64_t num_total_tuples = 0;
  std::vector<Sorter *> sorters;
  for (uint32_t sort_idx = 0; sort_idx < thread_states.NumThreads();
       sort_idx++) {
    auto *state = thread_states.AccessThreadState(sort_idx);
    auto *sorter = reinterpret_cast<Sorter *>(state + sorter_offset);
    num_total_tuples += sorter->NumTuples();
    sorters.push_back(sorter);
  }

  // Sort each run in parallel
  std::vector<std::vector<char *>> separators(sorters.size());
  for (uint32_t i = 0; i < separators.size(); i++) {
    separators[i].resize(sorters.size());
  }

  // Time
  Timer<std::milli> timer;
  timer.Start();

  common::synchronization::CountDownLatch sort_latch(sorters.size());
  auto &work_pool = threadpool::MonoQueuePool::GetExecutionInstance();
  for (uint32_t sort_idx = 0; sort_idx < sorters.size(); sort_idx++) {
    work_pool.SubmitTask([&sorters, &separators, &sort_latch, sort_idx]() {
      // First sort
      auto *sorter = sorters[sort_idx];
      sorter->Sort();

      // Now compute local separators
      auto part_size = sorter->NumTuples() / sorters.size();
      for (uint32_t idx = 0; idx < separators.size() - 1; idx++) {
        separators[idx][sort_idx] = sorter->tuples_[(idx + 1) * part_size];
      }

      // Count down latch
      sort_latch.CountDown();
    });
  }
  // Allocate room for new tuples
  tuples_.resize(num_total_tuples);

  // Wait sort jobs to be done
  sort_latch.Await(0);

  timer.Stop();
  LOG_DEBUG("Total sort time: %.2lf ms", timer.GetDuration());
  timer.Reset();
  timer.Start();

  // Where the merging work units are collected
  struct MergeWork {
    std::vector<std::pair<char **, char **>> input_ranges;
    char **destination = nullptr;
    MergeWork(std::vector<std::pair<char **, char **>> &&inputs, char **dest)
        : input_ranges(std::move(inputs)), destination(dest) {}
  };
  std::vector<MergeWork> merge_work;

  // Compute global separators from locally-computed separators
  auto cmp = [this](char *l, char *r) { return cmp_func_(l, r) < 0; };
  char **write_pos = tuples_.data();
  std::vector<char **> last_pos(sorters.size());
  for (uint32_t idx = 0; idx < separators.size(); idx++) {
    // Sort the local separators and choose the median
    char *separator = nullptr;
    if (idx < separators.size() - 1) {
      pdqsort(separators[idx].begin(), separators[idx].end(), cmp);
      separator = separators[idx][sorters.size() / 2];
    }

    std::vector<std::pair<char **, char **>> input_ranges;
    uint64_t part_size = 0;
    for (uint32_t sort_idx = 0; sort_idx < sorters.size(); sort_idx++) {
      Sorter *sorter = sorters[sort_idx];
      char **start = (idx == 0 ? sorter->tuples_.data() : last_pos[sort_idx]);
      char **end = sorter->tuples_.data() + sorter->tuples_.size();
      if (idx < separators.size() - 1) {
        end = std::upper_bound(start, end, separator, cmp);
      }
      if (start != end) {
        input_ranges.emplace_back(start, end);
      }
      part_size += (end - start);
      last_pos[sort_idx] = end;
    }
    // Add work
    merge_work.emplace_back(std::move(input_ranges), write_pos);
    // Bump new write position
    write_pos += part_size;
  }

  timer.Stop();
  LOG_DEBUG("Work generation time: %.2lf ms", timer.GetDuration());
  timer.Reset();
  timer.Start();

  common::synchronization::CountDownLatch merge_latch(merge_work.size());
  auto heap_cmp = [this](const std::pair<char **, char **> &l,
                         const std::pair<char **, char **> &r) {
    return !(cmp_func_(*l.first, *r.first) < 0);
  };
  for (auto &work : merge_work) {
    work_pool.SubmitTask([&work, &merge_latch, &heap_cmp, &cmp] {
      // Heap
      std::priority_queue<std::pair<char **, char **>,
                          std::vector<std::pair<char **, char **>>,
                          decltype(heap_cmp)> heap(heap_cmp, work.input_ranges);
      char **dest = work.destination;
      while (!heap.empty()) {
        auto top = heap.top();
        heap.pop();
        *dest++ = *top.first;
        if (top.first + 1 != top.second) {
          heap.emplace(top.first + 1, top.second);
        }
      }

      merge_latch.CountDown();
    });
  }
  // Wait
  merge_latch.Await(0);

  // Cleanup thread-local sorters by moving their allocated blocks to us
  for (auto *sorter : sorters) {
    blocks_.insert(blocks_.end(), sorter->blocks_.begin(),
                   sorter->blocks_.end());
    sorter->tuples_.clear();
    sorter->blocks_.clear();
  }

  tuples_start_ = tuples_.data();
  tuples_end_ = tuples_start_ + tuples_.size();

  timer.Stop();
  LOG_DEBUG("Merging sorted runs time: %.2lf ms", timer.GetDuration());
}

void Sorter::MakeRoomForNewTuple() {
  bool has_room =
      (buffer_pos_ != nullptr && buffer_pos_ + tuple_size_ < buffer_end_);
  if (has_room) {
    return;
  }

  PELOTON_ASSERT(next_alloc_size_ >= tuple_size_);

  LOG_TRACE("Allocating block of size %.2lf KB ...", next_alloc_size_ / 1024.0);

  // We need to allocate another block
  void *block = storage::BackendManager::GetInstance().Allocate(
      BackendType::MM, next_alloc_size_);
  PELOTON_ASSERT(block != nullptr);
  blocks_.emplace_back(block, next_alloc_size_);

  // Setup new buffer boundaries
  buffer_pos_ = reinterpret_cast<char *>(block);
  buffer_end_ = buffer_pos_ + next_alloc_size_;

  next_alloc_size_ *= 2;
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton
