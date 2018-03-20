/******************************************************************************
 * ips4o/ips4o_fwd.hpp
 *
 * In-place Parallel Super Scalar Samplesort (IPS⁴o)
 *
 ******************************************************************************
 * BSD 2-Clause License
 *
 * Copyright © 2017, Michael Axtmann <michael.axtmann@kit.edu>
 * Copyright © 2017, Daniel Ferizovic <daniel.ferizovic@student.kit.edu>
 * Copyright © 2017, Sascha Witt <sascha.witt@kit.edu>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *****************************************************************************/

#pragma once

#include <iterator>
#include <utility>

namespace ips4o {
namespace detail {

template <class It, class Comp>
inline void baseCaseSort(It begin, It end, Comp&& comp);

inline constexpr unsigned long log2(unsigned long n);

template <class It, class RandomGen>
inline void selectSample(It begin, const It end,
                         typename std::iterator_traits<It>::difference_type num_samples,
                         RandomGen&& gen);

template <class Cfg>
class Sorter {
 public:
    using iterator = typename Cfg::iterator;
    using diff_t = typename Cfg::difference_type;
    using value_type = typename Cfg::value_type;

    class BufferStorage;
    class Block;
    class Buffers;
    class BucketPointers;
    class Classifier;
    struct LocalData;
    struct SharedData;
    explicit Sorter(LocalData& local) : local_(local) {}

    void sequential(const iterator begin, const iterator end);

#if defined(_REENTRANT) || defined(_OPENMP)
    template <class TaskSorter>
    void parallelPrimary(const iterator begin, const iterator end, SharedData& shared,
                         int num_threads, TaskSorter&& task_sorter);

    void parallelSecondary(SharedData& shared, int id, int num_threads);
#endif

 private:
    LocalData& local_;
    SharedData* shared_;
    Classifier* classifier_;

    diff_t* bucket_start_;
    BucketPointers* bucket_pointers_;
    Block* overflow_;

    iterator begin_;
    iterator end_;
    int num_buckets_;
    int my_id_;
    int num_threads_;

    static inline int computeLogBuckets(const diff_t n);

    std::pair<int, bool> buildClassifier(const iterator begin, const iterator end,
                                         Classifier& classifier);

    template <bool kEqualBuckets> __attribute__((flatten))
    diff_t classifyLocally(const iterator my_begin, const iterator my_end);

    inline void parallelClassification(const bool use_equal_buckets);

    inline void sequentialClassification(const bool use_equal_buckets);

    void moveEmptyBlocks(const diff_t my_begin, const diff_t my_end,
                         const diff_t my_first_empty_block);

    inline int computeOverflowBucket();

    template <bool kEqualBuckets, bool kIsParallel>
    inline int classifyAndReadBlock(const int read_bucket);

    template <bool kEqualBuckets, bool kIsParallel>
    inline int swapBlock(const diff_t max_off, const int dest_bucket,
                         const bool current_swap);

    template <bool kEqualBuckets, bool kIsParallel>
    void permuteBlocks();

    inline std::pair<int, diff_t> saveMargins(int last_bucket);

    void writeMargins(const int first_bucket, const int last_bucket,
                      const int overflow_bucket, const int swap_bucket,
                      const diff_t in_swap_buffer);

    template <bool kIsParallel>
    std::pair<int, bool> partition(const iterator begin, const iterator end,
                                   diff_t* const bucket_start, SharedData* const shared,
                                   const int my_id, const int num_threads);

    inline void processSmallTasks(const iterator begin, SharedData& shared);
};

}  // namespace detail

template <class Cfg>
class SequentialSorter;

template <class Cfg>
class ParallelSorter;

template <class It, class Comp>
inline void sort(It begin, It end, Comp comp);

template <class It>
inline void sort(It begin, It end);

#if defined(_REENTRANT) || defined(_OPENMP)
namespace parallel {

template <class It, class Comp>
inline void sort(It begin, It end, Comp comp);

template <class It>
inline void sort(It begin, It end);

}  // namespace parallel
#endif
}  // namespace ips4o
