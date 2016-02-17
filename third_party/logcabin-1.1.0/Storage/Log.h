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
#include <memory>
#include <vector>

#include "build/Protocol/Raft.pb.h"
#include "build/Protocol/RaftLogMetadata.pb.h"

#ifndef LOGCABIN_STORAGE_LOG_H
#define LOGCABIN_STORAGE_LOG_H

namespace LogCabin {

// forward declaration
namespace Protocol {
class ServerStats;
}

namespace Storage {

/**
 * This interface is used by RaftConsensus to store log entries and metadata.
 * Typically, implementations will persist the log entries and metadata to
 * stable storage (but MemoryLog keeps it all in volatile memory).
 */
class Log {
  public:
    /**
     * An interface for flushing newly appended log entries to stable storage.
     * Leaders do this in a separate thread, while followers and candidates do
     * this immediately after appending the entries.
     *
     * Callers should wait() on all Sync objects prior to calling
     * truncateSuffix(). This never happens on leaders, so it's not a real
     * limitation, but things may go wonky otherwise.
     */
    class Sync {
      public:
        explicit Sync(uint64_t lastIndex);
        virtual ~Sync();
        /**
         * Wait for the log entries to be durable.
         * This is safe to call while the Log is being accessed and modified
         * from a separate thread.
         * PANICs on errors.
         */
        virtual void wait() {}
        /**
         * The index of the last log entry that is being flushed.
         * After the call to wait, every entry in the log up to this one is
         * durable.
         */
        uint64_t lastIndex;
        /**
         * Used by destructor to make sure that Log::syncComplete was called.
         */
        bool completed;
    };

    /**
     * The type of a log entry (the same format that's used in AppendEntries).
     */
    typedef Protocol::Raft::Entry Entry;

    Log();
    virtual ~Log();

    /**
     * Start to append new entries to the log. The entries may not be on disk
     * yet when this returns; see Sync.
     * \param entries
     *      Entries to place at the end of the log.
     * \return
     *      Range of indexes of the new entries in the log, inclusive.
     */
    virtual std::pair<uint64_t, uint64_t> append(
                            const std::vector<const Entry*>& entries) = 0;

    /**
     * Look up an entry by its log index.
     * \param index
     *      Must be in the range [getLogStartIndex(), getLastLogIndex()].
     *      Otherwise, this will crash the server.
     * \return
     *      The entry corresponding to that index. This reference is only
     *      guaranteed to be valid until the next time the log is modified.
     */
    virtual const Entry& getEntry(uint64_t index) const = 0;

    /**
     * Get the index of the first entry in the log (whether or not this
     * entry exists).
     * \return
     *      1 for logs that have never had truncatePrefix called,
     *      otherwise the largest index passed to truncatePrefix.
     */
    virtual uint64_t getLogStartIndex() const = 0;

    /**
     * Get the index of the most recent entry in the log.
     * \return
     *      The index of the most recent entry in the log,
     *      or getLogStartIndex() - 1 if the log is empty.
     */
    virtual uint64_t getLastLogIndex() const = 0;

    /**
     * Return the name of the log implementation as it would be specified in
     * the config file.
     */
    virtual std::string getName() const = 0;

    /**
     * Get the size of the entire log in bytes.
     */
    virtual uint64_t getSizeBytes() const = 0;

    /**
     * Release resources attached to the Sync object.
     * Call this after waiting on the Sync object.
     */
    void syncComplete(std::unique_ptr<Sync> sync) {
        sync->completed = true;
        syncCompleteVirtual(std::move(sync));
    }
  protected:
    /**
     * See syncComplete(). Intended for subclasses to override.
     */
    virtual void syncCompleteVirtual(std::unique_ptr<Sync> sync) {}
  public:

    /**
     * Get and remove the Log's Sync object in order to wait on it.
     * This Sync object must later be returned to the Log with syncComplete().
     *
     * While takeSync() and syncComplete() may not be done concurrently with
     * other Log operations, Sync::wait() may be done concurrently with all
     * operations except truncateSuffix().
     */
    virtual std::unique_ptr<Sync> takeSync() = 0;

    /**
     * Delete the log entries before the given index.
     * Once you truncate a prefix from the log, there's no way to undo this.
     * The entries may still be on disk when this returns and file descriptors
     * and other resources may remain open; see Sync.
     * \param firstIndex
     *      After this call, the log will contain no entries indexed less
     *      than firstIndex. This can be any log index, including 0 and those
     *      past the end of the log.
     */
    virtual void truncatePrefix(uint64_t firstIndex) = 0;

    /**
     * Delete the log entries past the given index.
     * This will not affect the log start index.
     * \param lastIndex
     *      After this call, the log will contain no entries indexed greater
     *      than lastIndex. This can be any log index, including 0 and those
     *      past the end of the log.
     * \warning
     *      Callers should wait() on all Sync object prior to calling
     *      truncateSuffix(). This never happens on leaders, so it's not a real
     *      limitation, but things may go wonky otherwise.
     */
    virtual void truncateSuffix(uint64_t lastIndex) = 0;

    /**
     * Call this after changing #metadata.
     */
    virtual void updateMetadata() = 0;

    /**
     * Add information about the log's state to the given structure.
     * Used for diagnostics.
     */
    virtual void updateServerStats(Protocol::ServerStats& serverStats) const {}

    /**
     * Opaque metadata that the log keeps track of.
     */
    Protocol::RaftLogMetadata::Metadata metadata;

    /**
     * Print out a Log for debugging purposes.
     */
    friend std::ostream& operator<<(std::ostream& os, const Log& log);

  protected:

    // Log is not copyable
    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;
};

} // namespace LogCabin::Storage
} // namespace LogCabin

#endif /* LOGCABIN_STORAGE_LOG_H */
