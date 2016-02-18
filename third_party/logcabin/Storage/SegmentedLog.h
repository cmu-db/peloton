/* Copyright (c) 2012-2014 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#include <deque>
#include <thread>
#include <vector>

#include "build/Storage/SegmentedLog.pb.h"
#include "Core/Buffer.h"
#include "Core/ConditionVariable.h"
#include "Core/Mutex.h"
#include "Core/RollingStat.h"
#include "Storage/FilesystemUtil.h"
#include "Storage/Log.h"

#ifndef LOGCABIN_STORAGE_SEGMENTEDLOG_H
#define LOGCABIN_STORAGE_SEGMENTEDLOG_H

namespace LogCabin {

// forward declaration
namespace Core {
class Config;
}

namespace Storage {

/**
 * This class persists a log on the filesystem efficiently.
 *
 * The log entries on disk are stored in a series of files called segments, and
 * each segment is about 8MB in size. Thus, most small appends do not need to
 * update filesystem metadata and can proceed with a single consecutive disk
 * write.
 *
 * The disk files consist of metadata files, closed segments, and open
 * segments. Metadata files are used to track Raft metadata, such as the
 * server's current term, and also the log's start index. Segments contain
 * contiguous entries that are part of the log. Closed segments are never
 * written to again (but may be renamed and truncated if a suffix of the log is
 * truncated). Open segments are where newly appended entries go. Once an open
 * segment reaches MAX_SEGMENT_SIZE, it is closed and a new one is used.
 *
 * Metadata files are named "metadata1" and "metadata2". The code alternates
 * between these so that there is always at least one readable metadata file.
 * On boot, the readable metadata file with the higher version number is used.
 *
 * Closed segments are named by the format string "%020lu-%020lu" with their
 * start and end indexes, both inclusive. Closed segments always contain at
 * least one entry; the end index is always at least as large as the start
 * index. Closed segment files may occasionally include data past their
 * filename's end index (these are ignored but a WARNING is issued). This can
 * happen if the suffix of the segment is truncated and a crash occurs at an
 * inopportune time (the segment file is first renamed, then truncated, and a
 * crash occurs in between).
 *
 * Open segments are named by the format string "open-%lu" with a unique
 * number. These should not exist when the server shuts down cleanly, but they
 * exist while the server is running and may be left around during a crash.
 * Open segments either contain entries which come after the last closed
 * segment or are full of zeros. When the server crashes while appending to an
 * open segment, the end of that file may be corrupt. We can't distinguish
 * between a corrupt file and a partially written entry. The code assumes it's
 * a partially written entry, issues a WARNING, and ignores it.
 *
 * Truncating a suffix of the log will remove all entries that are no longer
 * part of the log. Truncating a prefix of the log will only remove complete
 * segments that are before the new log start index. For example, if a
 * segment has entries 10 through 20 and the prefix of the log is truncated to
 * start at entry 15, that entire segment will be retained.
 *
 * Each segment file starts with a segment header, which currently contains
 * just a one-byte version number for the format of that segment. The current
 * format (version 1) is just a concatenation of serialized entry records.
 */
class SegmentedLog : public Log {
    /**
     * Clock used for measuring disk performance.
     */
    typedef Core::Time::SteadyClock Clock;
    /**
     * Time point for measuring disk performance.
     */
    typedef Clock::time_point TimePoint;

  public:

    /**
     * Specifies how individual records are serialized.
     */
    enum class Encoding {
      /// ProtoBuf human-readable text format.
      TEXT,
      /// ProtoBuf binary format.
      BINARY,
    };

    /**
     * Constructor.
     * \param parentDir
     *      A filesystem directory in which all the files for this storage
     *      module are kept.
     * \param encoding
     *      Specifies how individual records are stored.
     * \param config
     *      Settings.
     */
    SegmentedLog(const FilesystemUtil::File& parentDir,
                 Encoding encoding,
                 const Core::Config& config);
    ~SegmentedLog();

    // Methods implemented from Log interface
    std::pair<uint64_t, uint64_t>
    append(const std::vector<const Entry*>& entries);
    const Entry& getEntry(uint64_t) const;
    uint64_t getLogStartIndex() const;
    uint64_t getLastLogIndex() const;
    std::string getName() const;
    uint64_t getSizeBytes() const;
    std::unique_ptr<Log::Sync> takeSync();
    void syncCompleteVirtual(std::unique_ptr<Log::Sync> sync);
    void truncatePrefix(uint64_t firstIndex);
    void truncateSuffix(uint64_t lastIndex);
    void updateMetadata();
    void updateServerStats(Protocol::ServerStats& serverStats) const;

  private:

    /**
     * A producer/consumer monitor for a queue of files to use for open
     * segments. These are created asynchronously, hopefully ahead of the log
     * appends.
     *
     * This class is written in a monitor style; each public method acquires
     * #mutex.
     */
    class PreparedSegments {
      public:
        /**
         * The type of element that is queued in #openSegments.
         * The first element of each pair is its filename relative to #dir. The
         * second element is its open file descriptor.
         */
        typedef std::pair<std::string, FilesystemUtil::File> OpenSegment;

        /**
         * Constructor.
         * \param queueSize
         *      The maximum number of prepared segments to hold in the queue
         *      at a time.
         */
        explicit PreparedSegments(uint64_t queueSize);

        /**
         * Destructor.
         */
        ~PreparedSegments();

        /**
         * Do not block any more waiting threads, and return immediately.
         */
        void exit();

        /**
         * Ensure that future filenames will be larger than this one.
         * This should generally be invoked before any producers are started,
         * since once they're started, there's no stopping them.
         * \param fileId
         *      A lower bound on future IDs.
         */
        void foundFile(uint64_t fileId);

        /**
         * Immediately return all currently prepared segments.
         */
        std::deque<OpenSegment> releaseAll();

        /**
         * Producers call this when they're done creating a new file. This must
         * be called once after each call waitForDemand(); otherwise, the
         * internal bookkeeping won't work.
         * \param segment
         *      File to queue up for a consumer.
         */
        void submitOpenSegment(OpenSegment segment);

        /**
         * Producers call this first to block until work becomes needed.
         * \return
         *      ID for new filename.
         * \throw Core::Util::ThreadInterruptedException
         *      If exit() has been called.
         */
        uint64_t waitForDemand();

        /**
         * Consumers call this when they need a prepared segment file.
         * \throw Core::Util::ThreadInterruptedException
         *      If exit() has been called.
         */
        OpenSegment waitForOpenSegment();

        /**
         * Reduce log message verbosity for unit tests.
         */
        bool quietForUnitTests;

      private:
        /**
         * Mutual exclusion for all of the members of this class.
         */
        Core::Mutex mutex;
        /**
         * Notified when #openSegments shrinks in size or when #exiting becomes
         * true.
         */
        Core::ConditionVariable consumed;
        /**
         * Notified when #openSegments grows in size or when #exiting becomes
         * true.
         */
        Core::ConditionVariable produced;
        /**
         * Set to true when waiters should exit.
         */
        bool exiting;
        /**
         * The number of producers that may be started to fulfill demand.
         */
        uint64_t demanded;
        /**
         * Used to assign filenames to open segments (which is done before the
         * indexes they contain is known). This number is the largest of all
         * previously known numbers so that it can be incremented then assigned
         * to a new file. It's possible that numbers are reused across reboots.
         */
        uint64_t filenameCounter;
        /**
         * The queue where open segments sit before they're consumed. These are
         * available for the log to use as future open segments.
         */
        std::deque<OpenSegment> openSegments;
    };

    /**
     * Queues various operations on files, such as writes and fsyncs, to be
     * executed later.
     */
    class Sync : public Log::Sync {
      public:
        struct Op {
            enum OpCode {
                WRITE,
                TRUNCATE,
                RENAME,
                FDATASYNC,
                FSYNC,
                CLOSE,
                UNLINKAT,
                NOOP,
            };
            Op(int fd, OpCode opCode)
                : fd(fd)
                , opCode(opCode)
                , writeData()
                , filename1()
                , filename2()
                , size(0)
            {
            }
            int fd;
            OpCode opCode;
            Core::Buffer writeData;
            std::string filename1;
            std::string filename2;
            uint64_t size;
        };

        explicit Sync(uint64_t lastIndex,
                      std::chrono::nanoseconds diskWriteDurationThreshold);
        ~Sync();
        /**
         * Add how long the filesystem ops took to 'nanos'. This is invoked
         * from syncCompleteVirtual so that it is thread-safe with respect to
         * the 'nanos' variable. We can't do it in 'wait' directly since that
         * can execute concurrently with someone reading 'nanos'.
         */
        void updateStats(Core::RollingStat& nanos) const;
        /**
         * Called at the start of wait to avoid some redundant disk flushes.
         */
        void optimize();
        void wait();
        /// If a wait() exceeds this time, log a warning.
        const std::chrono::nanoseconds diskWriteDurationThreshold;
        /// List of operations to perform during wait().
        std::deque<Op> ops;
        /// Time at start of wait() call.
        TimePoint waitStart;
        /// Time at end of wait() call.
        TimePoint waitEnd;
    };

    /**
     * An open or closed segment. These are stored in #segmentsByStartIndex.
     */
    struct Segment {
        /**
         * Describes a log entry record within a segment.
         */
        struct Record {
            /**
             * Constructor.
             */
            explicit Record(uint64_t offset);

            /**
             * Byte offset in the file where the entry begins.
             * This is used when truncating a segment.
             */
            uint64_t offset;

            /**
             * The entry itself.
             */
            Log::Entry entry;
        };

        /**
         * Default constructor. Sets the segment to invalid.
         */
        Segment();

        /**
         * Return a filename of the right form for a closed segment.
         * See also #filename.
         */
        std::string makeClosedFilename() const;

        /**
         * True for the open segment, false for closed segments.
         */
        bool isOpen;
        /**
         * The index of the first entry in the segment. If the segment is open
         * and empty, this may not exist yet and will be #logStartIndex.
         */
        uint64_t startIndex;
        /**
         * The index of the last entry in the segment, or #startIndex - 1 if
         * the segment is open and empty.
         */
        uint64_t endIndex;
        /**
         * Size in bytes of the valid entries stored in the file plus
         * the version number at the start of the file.
         */
        uint64_t bytes;
        /**
         * The name of the file within #dir containing this segment.
         */
        std::string filename;
        /**
         * The entries in this segment, from startIndex to endIndex, inclusive.
         */
        std::deque<Record> entries;

    };

    /**
     * This goes at the start of every segment.
     */
    struct SegmentHeader {
        /**
         * Always set to 1 for now.
         */
        uint8_t version;
    } __attribute__((packed));

    ////////// initialization helper functions //////////

    /**
     * List the files in #dir and create Segment objects for any of them that
     * look like segments. This is only used during initialization. These
     * segments are passed through #loadClosedSegment() and #loadOpenSegment()
     * next.
     * Also updates SegmentPreparer::filenameCounter.
     * \return
     *      Partially initialized Segment objects, one per discovered filename.
     */
    std::vector<Segment> readSegmentFilenames();

    /**
     * Read a metadata file from disk. This is only used during initialization.
     * \param filename
     *      Filename within #dir to attempt to open and read.
     * \param[out] metadata
     *      Where the contents of the file end up.
     * \param quiet
     *      Set to true to avoid warnings when the file can't be read; used in
     *      unit tests.
     * \return
     *      True if the file was read successfully, false otherwise.
     */
    bool readMetadata(const std::string& filename,
                             SegmentedLogMetadata::Metadata& metadata,
                             bool quiet) const;

    /**
     * Read the given closed segment from disk, issuing PANICs and WARNINGs
     * appropriately. This is only used during initialization.
     *
     * Deletes segment if its last index is below logStartIndex.
     *
     * Reads every entry described in the filename, and PANICs if any of those
     * can't be read.
     *
     * \param[in,out] segment
     *      Closed segment to read from disk.
     * \param logStartIndex
     *      The index of the first entry in the log, according to the log
     *      metadata.
     * \return
     *      True if the segment is valid; false if it has been removed entirely
     *      from disk.
     */
    bool loadClosedSegment(Segment& segment, uint64_t logStartIndex);

    /**
     * Read the given open segment from disk, issuing PANICs and WARNINGs
     * appropriately, and closing the segment. This is only used during
     * initialization.
     *
     * Reads up through the end of the file or the last
     * entry with a valid checksum. If any valid entries are read, the segment
     * is truncated and closed. Otherwise, it is removed.
     *
     * Deletes segment if its last index is below logStartIndex.
     *
     * \param[in,out] segment
     *      Open segment to read from disk.
     * \param logStartIndex
     *      The index of the first entry in the log, according to the log
     *      metadata.
     * \return
     *      True if the segment is valid; false if it has been removed entirely
     *      from disk.
     */
    bool loadOpenSegment(Segment& segment, uint64_t logStartIndex);


    ////////// normal operation helper functions //////////

    /**
     * Run through a bunch of assertions of class invariants (for debugging).
     * For example, there should always be one open segment. See
     * #shouldCheckInvariants, controlled by the config option 'storageDebug',
     * and the BUILDTYPE.
     */
    void checkInvariants();

    /**
     * Close the open segment if one is open. This removes the open segment if
     * it is empty, or closes it otherwise. Since it's a class invariant that
     * there is always an open segment, the caller should open a new segment
     * after calling this (unless it's shutting down).
     */
    void closeSegment();

    /**
     * Return a reference to the current open segment (the one that new writes
     * should go into). Crashes if there is no open segment (but it's an
     * invariant of this class to maintain one).
     */
    Segment& getOpenSegment();
    const Segment& getOpenSegment() const;

    /**
     * Set up a new open segment for the log head.
     * This is called when #append() needs more space but also when the end of
     * the log is truncated with #truncatePrefix() or #truncateSuffix().
     * \pre
     *      There is no currently open segment.
     */
    void openNewSegment();

    /**
     * Read the next ProtoBuf record out of 'file'.
     * \param file
     *      The open file, useful for error messages.
     * \param reader
     *      A reader for 'file'.
     * \param[in,out] offset
     *      The byte offset in the file at which to start reading as input.
     *      The byte just after the last byte of data as output if successful,
     *      otherwise unmodified.
     * \param[out] out
     *      An empty ProtoBuf to fill in.
     * \return
     *      Empty string if successful, otherwise error message.
     *
     * Format:
     *
     * |checksum|dataLen|data|
     *
     * The checksum is up to Core::Checksum::MAX_LENGTH bytes and is terminated
     * by a null character. It covers both dataLen and data.
     *
     * dataLen is an unsigned integer (8 bytes, big-endian byte order) that
     * specifies the length in bytes of data.
     *
     * data is a protobuf encoded as binary or text, depending on encoding.
     */
    std::string readProtoFromFile(const FilesystemUtil::File& file,
                                  FilesystemUtil::FileContents& reader,
                                  uint64_t* offset,
                                  google::protobuf::Message* out) const;

    /**
     * Prepare a ProtoBuf record to be written to disk.
     * \param in
     *      ProtoBuf to be serialized.
     * \return
     *      Buffer containing serialized record.
     */
    Core::Buffer serializeProto(const google::protobuf::Message& in) const;

    ////////// segment preparer thread functions //////////

    /**
     * Opens a file for a new segment and allocates its space on disk.
     * \param fileId
     *      ID to use to generate filename; see
     *      SegmentPreparer::filenameCounter.
     * \return
     *      Filename and writable OS-level file.
     */
    std::pair<std::string, FilesystemUtil::File>
    prepareNewSegment(uint64_t fileId);

    /**
     * The main function for the #segmentPreparer thread.
     */
    void segmentPreparerMain();

    ////////// member variables //////////

    /**
     * Specifies how individual records are stored.
     */
    const Encoding encoding;

    /**
     * The algorithm to use when writing new records. When reading records, any
     * available checksum is used.
     */
    const std::string checksumAlgorithm;

    /**
     * The maximum size in bytes for newly written segments. Controlled by the
     * 'storageSegmentBytes' config option.
     */
    const uint64_t MAX_SEGMENT_SIZE;

    /**
     * Set to true if checkInvariants() should do its job, or set to false for
     * performance.
     */
    const bool shouldCheckInvariants;

    /**
     * If a disk operation exceeds this much time, log a warning.
     */
    const std::chrono::milliseconds diskWriteDurationThreshold;

    /**
     * The metadata this class mintains. This should be combined with the
     * superclass's metadata when being written out to disk.
     */
    SegmentedLogMetadata::Metadata metadata;

    /**
     * The directory containing every file this log creates.
     */
    FilesystemUtil::File dir;

    /**
     * A writable OS-level file that contains the entries for the current open
     * segment. It is a class invariant that this is always a valid file.
     */
    FilesystemUtil::File openSegmentFile;

    /**
     * The index of the first entry in the log, see getLogStartIndex().
     */
    uint64_t logStartIndex;

    /**
     * Ordered map of all closed segments and the open segment, indexed by the
     * startIndex of each segment. This is used to support all the key
     * operations, such as looking up an entry and truncation.
     */
    std::map<uint64_t, Segment> segmentsByStartIndex;

    /**
     * The total number of bytes occupied by the closed segments on disk.
     * Used to calculate getSizeBytes() efficiently.
     */
    uint64_t totalClosedSegmentBytes;

    /**
     * See PreparedSegments.
     */
    PreparedSegments preparedSegments;

    /**
     * Accumulates deferred filesystem operations for append() and
     * truncatePrefix().
     */
    std::unique_ptr<SegmentedLog::Sync> currentSync;

    /**
     * Tracks the time it takes to write a metadata file.
     */
    Core::RollingStat metadataWriteNanos;

    /**
     * Tracks the time it takes to execute wait() on a Sync object.
     */
    Core::RollingStat filesystemOpsNanos;

    /**
     * Opens files, allocates the to full size, and places them on
     * #preparedSegments for the log to use.
     */
    std::thread segmentPreparer;
};

} // namespace LogCabin::Storage
} // namespace LogCabin

#endif /* LOGCABIN_STORAGE_SEGMENTEDLOG_H */
