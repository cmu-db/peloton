/* Copyright (c) 2012-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <cinttypes>
#include <deque>
#include <memory>
#include <string>

#include "Storage/Log.h"

#ifndef LOGCABIN_STORAGE_MEMORYLOG_H
#define LOGCABIN_STORAGE_MEMORYLOG_H

namespace LogCabin {
namespace Storage {

// forward declaration
class Globals;

class MemoryLog : public Log {
  public:
    MemoryLog();
    ~MemoryLog();

    std::pair<uint64_t, uint64_t>
    append(const std::vector<const Entry*>& entries);
    const Entry& getEntry(uint64_t logIndex) const;
    uint64_t getLogStartIndex() const;
    uint64_t getLastLogIndex() const;
    std::string getName() const;
    uint64_t getSizeBytes() const;
    std::unique_ptr<Sync> takeSync();
    void truncatePrefix(uint64_t firstIndex);
    void truncateSuffix(uint64_t lastIndex);
    void updateMetadata();

  protected:

    /**
     * The index for the first entry in the log. Begins as 1 for new logs but
     * will be larger for logs that have been snapshotted.
     */
    uint64_t startIndex;

    /**
     * Stores the entries that make up the log.
     * The offset into 'entries' is the index of the entry minus 'startIndex'.
     * This is a deque rather than a vector to support fast prefix truncation
     * (used after snapshotting a prefix of the log).
     */
    std::deque<Entry> entries;

    /**
     * This is returned by the next call to getSync.
     * It's totally unnecessary to have this member for MemoryLog, as its syncs
     * don't do anything. However, it's useful for injecting different times of
     * Syncs into unit tests.
     */
    std::unique_ptr<Sync> currentSync;
};

} // namespace LogCabin::Storage
} // namespace LogCabin

#endif /* LOGCABIN_STORAGE_MEMORYLOG_H */
