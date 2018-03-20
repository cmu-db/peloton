/******************************************************************************
 * ips4o/empty_block_movement.hpp
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

#include <algorithm>

#include "ips4o_fwd.hpp"
#include "memory.hpp"

namespace ips4o {
namespace detail {

/**
 * Moves empty blocks to establish invariant.
 */
template <class Cfg>
void Sorter<Cfg>::moveEmptyBlocks(const diff_t my_begin, const diff_t my_end,
                                  const diff_t my_first_empty_block) {
    // First and last buckets that end in this stripe
    const int my_first_bucket = [&](int i) {
        while (bucket_start_[i] < my_begin && bucket_start_[i + 1] <= my_begin) ++i;
        return i;
    }(0);
    const int my_last_bucket = [&](int i) {
        const auto num_buckets = num_buckets_;
        while (i < num_buckets && bucket_start_[i + 1] <= my_end) ++i;
        return i;
    }(my_first_bucket);

    const auto bstart = Cfg::alignToNextBlock(bucket_start_[my_last_bucket]);

    // Fix the last bucket if it extends over the stripe boundary
    if (bstart < my_end) {
        // Count how many blocks we have to fill
        const auto blocks_to_fill = my_end - std::max(my_first_empty_block, bstart);

        // If it is a very large bucket, other threads will also move blocks around in it
        diff_t blocks_reserved = 0;
        if (bstart < my_begin) {
            int prev_id = my_id_ - 1;
            do {
                const auto eb = shared_->local[prev_id]->first_empty_block;
                blocks_reserved += shared_->local[prev_id + 1]->first_block - std::max(eb, bstart);
            } while (prev_id-- && bstart < shared_->local[prev_id + 1]->first_block);
        }

        // Find stripe in which bucket ends
        const auto bend = Cfg::alignToNextBlock(bucket_start_[my_last_bucket + 1]);
        const auto elements_per_thread = static_cast<double>(end_ - begin_) / num_threads_;
        int read_stripe = (bucket_start_[my_last_bucket + 1] - 1) / elements_per_thread;

        auto write = begin_ + std::max(my_first_empty_block, bstart);
        const auto write_end = write + blocks_to_fill;

        diff_t read = -1, read_end = 0;
        const auto updateReadPointers = [&] {
            // Find next block to read in previous stripe when the current stripe is empty
            while (read <= read_end) {
                --read_stripe;
                read = std::min(shared_->local[read_stripe]->first_empty_block, bend) - Cfg::kBlockSize;
                read_end = shared_->local[read_stripe]->first_block - Cfg::kBlockSize;
                if (read_stripe == my_id_) return false;
            }
            return true;
        };
        ++read_stripe;

        // Read pointer will get updated one more time after last block has been written
        while (updateReadPointers() && write < write_end) {
            // Skip reserved blocks
            if (blocks_reserved >= (read - read_end)) {
                blocks_reserved -= (read - read_end);
                read = read_end;
                continue;
            }
            read -= blocks_reserved;
            blocks_reserved = 0;
            // Move blocks from end of bucket into gap
            while (read > read_end && write < write_end) {
                std::move(begin_ + read, begin_ + read + Cfg::kBlockSize, write);
                read -= Cfg::kBlockSize;
                write += Cfg::kBlockSize;
            }
        }

        // Set bucket pointers if the last filled block in the bucket is in our stripe
        if (write == write_end) {
            if (read_stripe == my_id_ + 1) bucket_pointers_[my_last_bucket].set(bstart, read);
            else if (read_stripe == my_id_) bucket_pointers_[my_last_bucket].set(bstart, write - begin_ - Cfg::kBlockSize);
        } else if (write > begin_ + std::max(my_first_empty_block, bstart) || bstart >= my_begin) {
            bucket_pointers_[my_last_bucket].set(bstart, write - begin_ - Cfg::kBlockSize);
        }
    }

    // Only set the pointers for the first bucket if no other thread does it
    const auto exclude_first = my_first_bucket == my_last_bucket
                               || Cfg::alignToNextBlock(bucket_start_[my_first_bucket]) < my_begin;
    for (int b = my_first_bucket + exclude_first; b < my_last_bucket; ++b) {
        const auto start = Cfg::alignToNextBlock(bucket_start_[b]);
        const auto stop = Cfg::alignToNextBlock(bucket_start_[b + 1]);
        const auto read = (start >= my_first_empty_block ? start
                                                         : std::min(stop, my_first_empty_block))
                           - Cfg::kBlockSize;
        bucket_pointers_[b].set(start, read);
    }
}

}  // namespace detail
}  // namespace ips4o
