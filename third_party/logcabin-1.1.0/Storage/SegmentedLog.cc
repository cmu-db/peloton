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

#define _BSD_SOURCE
#include <endian.h>

#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "build/Protocol/Raft.pb.h"
#include "Core/Checksum.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Core/StringUtil.h"
#include "Core/ThreadId.h"
#include "Core/Time.h"
#include "Core/Util.h"
#include "Storage/FilesystemUtil.h"
#include "Storage/SegmentedLog.h"
#include "Server/Globals.h"

namespace LogCabin {
namespace Storage {

namespace FS = FilesystemUtil;
using Core::StringUtil::format;

namespace {

/**
 * Format string for open segment filenames.
 * First param: incrementing counter.
 */
#define OPEN_SEGMENT_FORMAT "open-%lu"

/**
 * Format string for closed segment filenames.
 * First param: start index, inclusive.
 * Second param: end index, inclusive.
 */
#define CLOSED_SEGMENT_FORMAT "%020lu-%020lu"

/**
 * Return true if all the bytes in range [start, start + length) are zero.
 */
bool
isAllZeros(const void* _start, size_t length)
{
    const uint8_t* start = static_cast<const uint8_t*>(_start);
    for (size_t offset = 0; offset < length; ++offset) {
        if (start[offset] != 0)
            return false;
    }
    return true;
}

} // anonymous namespace


////////// SegmentedLog::PreparedSegments //////////


SegmentedLog::PreparedSegments::PreparedSegments(uint64_t queueSize)
    : quietForUnitTests(false)
    , mutex()
    , consumed()
    , produced()
    , exiting(false)
    , demanded(queueSize)
    , filenameCounter(0)
    , openSegments()
{
}

SegmentedLog::PreparedSegments::~PreparedSegments()
{
}

void
SegmentedLog::PreparedSegments::exit()
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    exiting = true;
    consumed.notify_all();
    produced.notify_all();
}

void
SegmentedLog::PreparedSegments::foundFile(uint64_t fileId)
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    if (filenameCounter < fileId)
        filenameCounter = fileId;
}

std::deque<SegmentedLog::PreparedSegments::OpenSegment>
SegmentedLog::PreparedSegments::releaseAll()
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    std::deque<OpenSegment> ret;
    std::swap(openSegments, ret);
    return ret;
}

void
SegmentedLog::PreparedSegments::submitOpenSegment(OpenSegment segment)
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    openSegments.push_back(std::move(segment));
    produced.notify_one();
}

uint64_t
SegmentedLog::PreparedSegments::waitForDemand()
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    while (!exiting) {
        if (demanded > 0) {
            --demanded;
            ++filenameCounter;
            return filenameCounter;
        }
        consumed.wait(lockGuard);
    }
    throw Core::Util::ThreadInterruptedException();
}

SegmentedLog::PreparedSegments::OpenSegment
SegmentedLog::PreparedSegments::waitForOpenSegment()
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    uint64_t numWaits = 0;
    while (true) {
        if (exiting) {
            NOTICE("Exiting");
            throw Core::Util::ThreadInterruptedException();
        }
        if (!openSegments.empty()) {
            break;
        }
        if (numWaits == 0 && !quietForUnitTests) {
            WARNING("Prepared segment not ready, having to wait on it. "
                    "This is perfectly safe but bad for performance. "
                    "Consider increasing storageOpenSegments in the config.");
        }
        ++numWaits;
        produced.wait(lockGuard);
    }
    if (numWaits > 0 && !quietForUnitTests) {
        WARNING("Done waiting: prepared segment now ready");
    }
    OpenSegment r = std::move(openSegments.front());
    openSegments.pop_front();
    consumed.notify_one();
    ++demanded;
    return r;
}


////////// SegmentedLog::Sync //////////


SegmentedLog::Sync::Sync(uint64_t lastIndex,
                         std::chrono::nanoseconds diskWriteDurationThreshold)
    : Log::Sync(lastIndex)
    , diskWriteDurationThreshold(diskWriteDurationThreshold)
    , ops()
    , waitStart(TimePoint::max())
    , waitEnd(TimePoint::max())
{
}

SegmentedLog::Sync::~Sync()
{
}

void
SegmentedLog::Sync::optimize()
{
    if (ops.size() < 3)
        return;
    auto prev = ops.begin();
    auto it = prev + 1;
    auto next = it + 1;
    while (next != ops.end()) {
        if (prev->opCode == Op::FDATASYNC &&
            it->opCode == Op::WRITE &&
            next->opCode == Op::FDATASYNC &&
            prev->fd == it->fd &&
            it->fd == next->fd) {
            prev->opCode = Op::NOOP;
        }
        prev = it;
        it = next;
        ++next;
    }
}

void
SegmentedLog::Sync::wait()
{
    optimize();

    waitStart = Clock::now();
    uint64_t writes = 0;
    uint64_t totalBytesWritten = 0;
    uint64_t truncates = 0;
    uint64_t renames = 0;
    uint64_t fdatasyncs = 0;
    uint64_t fsyncs = 0;
    uint64_t closes = 0;
    uint64_t unlinks = 0;

    while (!ops.empty()) {
        Op& op = ops.front();
        FS::File f(op.fd, "-unknown-");
        switch (op.opCode) {
            case Op::WRITE: {
                ssize_t written = FS::write(op.fd,
                        op.writeData.getData(),
                        op.writeData.getLength());
                if (written < 0) {
                    PANIC("Failed to write to fd %d: %s",
                          op.fd,
                          strerror(errno));
                }
                ++writes;
                totalBytesWritten += op.writeData.getLength();
                break;
            }
            case Op::TRUNCATE: {
                FS::truncate(f, op.size);
                ++truncates;
                break;
            }
            case Op::RENAME: {
                FS::rename(f, op.filename1,
                           f, op.filename2);
                ++renames;
                break;
            }
            case Op::FDATASYNC: {
                FS::fdatasync(f);
                ++fdatasyncs;
                break;
            }
            case Op::FSYNC: {
                FS::fsync(f);
                ++fsyncs;
                break;
            }
            case Op::CLOSE: {
                f.close();
                ++closes;
                break;
            }
            case Op::UNLINKAT: {
                FS::removeFile(f, op.filename1);
                ++unlinks;
                break;
            }
            case Op::NOOP: {
                break;
            }
        }
        f.release();
        ops.pop_front();
    }

    waitEnd = Clock::now();
    std::chrono::nanoseconds elapsed = waitEnd - waitStart;
    if (elapsed > diskWriteDurationThreshold) {
        WARNING("Executing filesystem operations took longer than expected "
                "(%s for %lu writes totaling %lu bytes, %lu truncates, "
                "%lu renames, %lu fdatasyncs, %lu fsyncs, %lu closes, and "
                "%lu unlinks)",
                Core::StringUtil::toString(elapsed).c_str(),
                writes,
                totalBytesWritten,
                truncates,
                renames,
                fdatasyncs,
                fsyncs,
                closes,
                unlinks);
    }
}

void
SegmentedLog::Sync::updateStats(Core::RollingStat& nanos) const
{
    std::chrono::nanoseconds elapsed = waitEnd - waitStart;
    nanos.push(uint64_t(elapsed.count()));
    if (elapsed > diskWriteDurationThreshold)
        nanos.noteExceptional(waitStart, uint64_t(elapsed.count()));
}


////////// SegmentedLog::Segment::Record //////////


SegmentedLog::Segment::Record::Record(uint64_t offset)
    : offset(offset)
    , entry()
{
}


////////// SegmentedLog::Segment //////////

SegmentedLog::Segment::Segment()
    : isOpen(false)
    , startIndex(~0UL)
    , endIndex(~0UL - 1)
    , bytes(0)
    , filename("--invalid--")
    , entries()
{
}

std::string
SegmentedLog::Segment::makeClosedFilename() const
{
    return format(CLOSED_SEGMENT_FORMAT,
                  startIndex, endIndex);
}

////////// SegmentedLog public functions //////////


SegmentedLog::SegmentedLog(const FS::File& parentDir,
                           Encoding encoding,
                           const Core::Config& config)
    : encoding(encoding)
    , checksumAlgorithm(config.read<std::string>("storageChecksum", "CRC32"))
    , MAX_SEGMENT_SIZE(config.read<uint64_t>("storageSegmentBytes",
                                             8 * 1024 * 1024))
    , shouldCheckInvariants(config.read<bool>("storageDebug", false))
    , diskWriteDurationThreshold(config.read<uint64_t>(
        "electionTimeoutMilliseconds", 500) / 4)
    , metadata()
    , dir(FS::openDir(parentDir,
                      (encoding == Encoding::BINARY
                        ? "Segmented-Binary"
                        : "Segmented-Text")))
    , openSegmentFile()
    , logStartIndex(1)
    , segmentsByStartIndex()
    , totalClosedSegmentBytes(0)
    , preparedSegments(
        std::max(config.read<uint64_t>("storageOpenSegments", 3),
                 1UL))
    , currentSync(new SegmentedLog::Sync(0, diskWriteDurationThreshold))
    , metadataWriteNanos()
    , filesystemOpsNanos()
    , segmentPreparer()
{
    std::vector<Segment> segments = readSegmentFilenames();

    bool quiet = config.read<bool>("unittest-quiet", false);
    preparedSegments.quietForUnitTests = quiet;
    SegmentedLogMetadata::Metadata metadata1;
    SegmentedLogMetadata::Metadata metadata2;
    bool ok1 = readMetadata("metadata1", metadata1, quiet);
    bool ok2 = readMetadata("metadata2", metadata2, quiet);
    if (ok1 && ok2) {
        if (metadata1.version() > metadata2.version())
            metadata = metadata1;
        else
            metadata = metadata2;
    } else if (ok1) {
        metadata = metadata1;
    } else if (ok2) {
        metadata = metadata2;
    } else {
        // Brand new servers won't have metadata, and that's ok.
        if (!segments.empty()) {
            PANIC("No readable metadata file but found segments in %s",
                  dir.path.c_str());
        }
        metadata.set_entries_start(logStartIndex);
    }

    logStartIndex = metadata.entries_start();
    Log::metadata = metadata.raft_metadata();
    // Write both metadata files
    updateMetadata();
    updateMetadata();
    FS::fsync(dir); // in case metadata files didn't exist


    // Read data from segments, closing any open segments.
    for (auto it = segments.begin(); it != segments.end(); ++it) {
        Segment& segment = *it;
        bool keep = segment.isOpen ? loadOpenSegment(segment, logStartIndex)
                                   : loadClosedSegment(segment, logStartIndex);
        if (keep) {
            assert(!segment.isOpen);
            uint64_t startIndex = segment.startIndex;
            std::string filename = segment.filename;
            auto result = segmentsByStartIndex.insert({startIndex,
                                                       std::move(segment)});
            if (!result.second) {
                Segment& other = result.first->second;
                PANIC("Two segments contain entry %lu: %s and %s",
                      startIndex,
                      other.filename.c_str(),
                      filename.c_str());
            }
        }
    }

    // Check to make sure no entry is present in more than one segment,
    // and that there's no gap in the numbering for entries we have.
    if (!segmentsByStartIndex.empty()) {
        uint64_t nextIndex = segmentsByStartIndex.begin()->first;
        for (auto it = segmentsByStartIndex.begin();
             it != segmentsByStartIndex.end();
             ++it) {
            Segment& segment = it->second;
            if (nextIndex < segment.startIndex) {
                PANIC("Did not find segment containing entries "
                      "%lu to %lu (inclusive)",
                      nextIndex, segment.startIndex - 1);
            } else if (segment.startIndex < nextIndex) {
                PANIC("Segment %s contains duplicate entries "
                      "%lu to %lu (inclusive)",
                      segment.filename.c_str(),
                      segment.startIndex,
                      std::min(segment.endIndex,
                               nextIndex - 1));
            }
            nextIndex = segment.endIndex + 1;
        }
    }

    // Open a segment to write new entries into.
    uint64_t fileId = preparedSegments.waitForDemand();
    preparedSegments.submitOpenSegment(
        prepareNewSegment(fileId));
    openNewSegment();

    // Launch the segment preparer thread so that we'll have a source for
    // additional new segments.
    segmentPreparer = std::thread(&SegmentedLog::segmentPreparerMain, this);

    checkInvariants();
}

SegmentedLog::~SegmentedLog()
{
    NOTICE("Closing open segment");
    closeSegment();

    // Stop preparing segments and delete the extras.
    preparedSegments.exit();
    if (segmentPreparer.joinable())
        segmentPreparer.join();
    auto prepared = preparedSegments.releaseAll();
    while (!prepared.empty()) {
        std::string filename = prepared.front().first;
        NOTICE("Removing unused open segment: %s",
               filename.c_str());
        FS::removeFile(dir, filename);
        prepared.pop_front();
    }
    FS::fsync(dir);

    // Keep assertion in Log.h happy. No need to "take" and "complete" this
    // sync since no operations were performed.
    if (currentSync->ops.empty())
        currentSync->completed = true;
}

std::pair<uint64_t, uint64_t>
SegmentedLog::append(const std::vector<const Entry*>& entries)
{
    Segment* openSegment = &getOpenSegment();
    uint64_t startIndex = openSegment->endIndex + 1;
    uint64_t index = startIndex;
    for (auto it = entries.begin(); it != entries.end(); ++it) {
        Segment::Record record(openSegment->bytes);
        // Note that record.offset may change later, if this entry doesn't fit.
        record.entry = **it;
        if (record.entry.has_index()) {
            assert(index == record.entry.index());
        } else {
            record.entry.set_index(index);
        }
        Core::Buffer buf = serializeProto(record.entry);

        // See if we need to roll over to a new head segment. If someone is
        // writing an entry that is bigger than MAX_SEGMENT_SIZE, just put it
        // in its own segment. This duplicates some code from closeSegment(),
        // but queues up the operations into 'currentSync'.
        if (openSegment->bytes > sizeof(SegmentHeader) &&
            openSegment->bytes + buf.getLength() > MAX_SEGMENT_SIZE) {
            NOTICE("Rolling over to new head segment: trying to append new "
                   "entry that is %lu bytes long, but open segment is already "
                   "%lu of %lu bytes large",
                   buf.getLength(),
                   openSegment->bytes,
                   MAX_SEGMENT_SIZE);

            // Truncate away any extra 0 bytes at the end from when
            // MAX_SEGMENT_SIZE was allocated.
            currentSync->ops.emplace_back(openSegmentFile.fd,
                                          Sync::Op::TRUNCATE);
            currentSync->ops.back().size = openSegment->bytes;
            currentSync->ops.emplace_back(openSegmentFile.fd,
                                          Sync::Op::FSYNC);
            currentSync->ops.emplace_back(openSegmentFile.release(),
                                          Sync::Op::CLOSE);

            // Rename the file.
            std::string newFilename = openSegment->makeClosedFilename();
            NOTICE("Closing full segment (was %s, renaming to %s)",
                   openSegment->filename.c_str(),
                   newFilename.c_str());
            currentSync->ops.emplace_back(dir.fd, Sync::Op::RENAME);
            currentSync->ops.back().filename1 = openSegment->filename;
            currentSync->ops.back().filename2 = newFilename;
            currentSync->ops.emplace_back(dir.fd, Sync::Op::FSYNC);
            openSegment->filename = newFilename;

            // Bookkeeping.
            openSegment->isOpen = false;
            totalClosedSegmentBytes += openSegment->bytes;

            // Open new segment.
            openNewSegment();
            openSegment = &getOpenSegment();
            record.offset = openSegment->bytes;
        }

        if (buf.getLength() > MAX_SEGMENT_SIZE) {
            WARNING("Trying to append an entry of %lu bytes when the maximum "
                    "segment size is %lu bytes. Placing this entry in its own "
                    "segment. Consider adjusting 'storageSegmentBytes' in the "
                    "config.",
                    buf.getLength(),
                    MAX_SEGMENT_SIZE);
        }

        openSegment->entries.emplace_back(std::move(record));
        openSegment->bytes += buf.getLength();
        currentSync->ops.emplace_back(openSegmentFile.fd, Sync::Op::WRITE);
        currentSync->ops.back().writeData = std::move(buf);
        ++openSegment->endIndex;
        ++index;
    }

    currentSync->ops.emplace_back(openSegmentFile.fd, Sync::Op::FDATASYNC);
    currentSync->lastIndex = getLastLogIndex();
    checkInvariants();
    return {startIndex, getLastLogIndex()};
}

const SegmentedLog::Entry&
SegmentedLog::getEntry(uint64_t index) const
{
    if (index < getLogStartIndex() ||
        index > getLastLogIndex()) {
        PANIC("Attempted to access entry %lu outside of log "
              "(start index is %lu, last index is %lu)",
              index, getLogStartIndex(), getLastLogIndex());
    }
    auto it = segmentsByStartIndex.upper_bound(index);
    --it;
    const Segment& segment = it->second;
    assert(segment.startIndex <= index);
    assert(index <= segment.endIndex);
    return segment.entries.at(index - segment.startIndex).entry;
}

uint64_t
SegmentedLog::getLogStartIndex() const
{
    return logStartIndex;
}

uint64_t
SegmentedLog::getLastLogIndex() const
{
    // Although it's a class invariant that there's always an open segment,
    // it's convenient to be able to call this as a helper function when there
    // are no segments.
    if (segmentsByStartIndex.empty())
        return logStartIndex - 1;
    else
        return getOpenSegment().endIndex;
}

std::string
SegmentedLog::getName() const
{
    if (encoding == Encoding::BINARY)
        return "Segmented-Binary";
    else
        return "Segmented-Text";
}

uint64_t
SegmentedLog::getSizeBytes() const
{
    return totalClosedSegmentBytes + getOpenSegment().bytes;
}

std::unique_ptr<Log::Sync>
SegmentedLog::takeSync()
{
    std::unique_ptr<SegmentedLog::Sync> other(
            new SegmentedLog::Sync(getLastLogIndex(),
                                   diskWriteDurationThreshold));
    std::swap(other, currentSync);
    return std::move(other);
}

void
SegmentedLog::syncCompleteVirtual(std::unique_ptr<Log::Sync> sync)
{
    static_cast<SegmentedLog::Sync*>(sync.get())->
        updateStats(filesystemOpsNanos);
}

void
SegmentedLog::truncatePrefix(uint64_t newStartIndex)
{
    if (newStartIndex <= logStartIndex)
        return;

    NOTICE("Truncating log to start at index %lu (was %lu)",
           newStartIndex, logStartIndex);
    logStartIndex = newStartIndex;
    // update metadata before removing files in case of interruption
    updateMetadata();

    while (!segmentsByStartIndex.empty()) {
        Segment& segment = segmentsByStartIndex.begin()->second;
        if (logStartIndex <= segment.endIndex)
            break;
        NOTICE("Deleting unneeded segment %s (its end index is %lu)",
               segment.filename.c_str(),
               segment.endIndex);
        currentSync->ops.emplace_back(dir.fd, Sync::Op::UNLINKAT);
        currentSync->ops.back().filename1 = segment.filename;
        if (segment.isOpen) {
            currentSync->ops.emplace_back(openSegmentFile.release(),
                                          Sync::Op::CLOSE);
        } else {
            totalClosedSegmentBytes -= segment.bytes;
        }
        segmentsByStartIndex.erase(segmentsByStartIndex.begin());
    }

    if (segmentsByStartIndex.empty())
        openNewSegment();
    if (currentSync->lastIndex < logStartIndex - 1)
        currentSync->lastIndex = logStartIndex - 1;
    checkInvariants();
}

void
SegmentedLog::truncateSuffix(uint64_t newEndIndex)
{
    if (newEndIndex >= getLastLogIndex())
        return;

    NOTICE("Truncating log to end at index %lu (was %lu)",
           newEndIndex, getLastLogIndex());
    { // Check if the open segment has some entries we need. If so,
      // just truncate that segment, open a new one, and return.
        Segment& openSegment = getOpenSegment();
        if (newEndIndex >= openSegment.startIndex) {
            // Update in-memory segment
            uint64_t i = newEndIndex + 1 - openSegment.startIndex;
            openSegment.bytes = openSegment.entries.at(i).offset;
            openSegment.entries.erase(
                openSegment.entries.begin() + int64_t(i),
                openSegment.entries.end());
            openSegment.endIndex = newEndIndex;
            // Truncate and close the open segment, and open a new one.
            closeSegment();
            openNewSegment();
            checkInvariants();
            return;
        }
    }

    { // Remove the open segment.
        Segment& openSegment = getOpenSegment();
        openSegment.endIndex = openSegment.startIndex - 1;
        openSegment.bytes = 0;
        closeSegment();
    }

    // Remove and/or truncate closed segments.
    while (!segmentsByStartIndex.empty()) {
        auto it = segmentsByStartIndex.rbegin();
        Segment& segment = it->second;
        if (segment.endIndex == newEndIndex)
            break;
        if (segment.startIndex > newEndIndex) { // remove segment
            NOTICE("Removing closed segment %s", segment.filename.c_str());
            FS::removeFile(dir, segment.filename);
            FS::fsync(dir);
            totalClosedSegmentBytes -= segment.bytes;
            segmentsByStartIndex.erase(segment.startIndex);
        } else if (segment.endIndex > newEndIndex) { // truncate segment
            // Update in-memory segment
            uint64_t i = newEndIndex + 1 - segment.startIndex;
            uint64_t newBytes = segment.entries.at(i).offset;
            totalClosedSegmentBytes -= (segment.bytes - newBytes);
            segment.bytes = newBytes;
            segment.entries.erase(
                segment.entries.begin() + int64_t(i),
                segment.entries.end());
            segment.endIndex = newEndIndex;

            // Rename the file
            std::string newFilename = segment.makeClosedFilename();
            NOTICE("Truncating closed segment (was %s, renaming to %s)",
                   segment.filename.c_str(),
                   newFilename.c_str());
            FS::rename(dir, segment.filename,
                       dir, newFilename);
            FS::fsync(dir);
            segment.filename = newFilename;

            // Truncate the file
            FS::File f = FS::openFile(dir, segment.filename, O_WRONLY);
            FS::truncate(f, segment.bytes);
            FS::fsync(f);
        }
    }

    // Reopen a segment (so that we can write again)
    openNewSegment();
    checkInvariants();
}

void
SegmentedLog::updateMetadata()
{
    if (Log::metadata.ByteSize() == 0)
        metadata.clear_raft_metadata();
    else
        *metadata.mutable_raft_metadata() = Log::metadata;
    metadata.set_format_version(1);
    metadata.set_entries_start(logStartIndex);
    metadata.set_version(metadata.version() + 1);
    std::string filename;
    if (metadata.version() % 2 == 1) {
        filename = "metadata1";
    } else {
        filename = "metadata2";
    }

    TimePoint start = Clock::now();

    NOTICE("Writing new storage metadata (version %lu) to %s",
           metadata.version(),
           filename.c_str());
    FS::File file = FS::openFile(dir, filename, O_CREAT|O_WRONLY|O_TRUNC);
    Core::Buffer record = serializeProto(metadata);
    ssize_t written = FS::write(file.fd,
                                record.getData(),
                                record.getLength());
    if (written == -1) {
        PANIC("Failed to write to %s: %s",
              file.path.c_str(), strerror(errno));
    }
    FS::fsync(file);

    TimePoint end = Clock::now();
    std::chrono::nanoseconds elapsed = end - start;
    metadataWriteNanos.push(uint64_t(elapsed.count()));
    if (elapsed > diskWriteDurationThreshold) {
        WARNING("Writing metadata file took longer than expected "
                "(%s for %lu bytes)",
                Core::StringUtil::toString(elapsed).c_str(),
                record.getLength());
        metadataWriteNanos.noteExceptional(start, uint64_t(elapsed.count()));
    }
}

void
SegmentedLog::updateServerStats(Protocol::ServerStats& serverStats) const
{
    Protocol::ServerStats::Storage& stats = *serverStats.mutable_storage();
    stats.set_num_segments(segmentsByStartIndex.size());
    stats.set_open_segment_bytes(getOpenSegment().bytes);
    stats.set_metadata_version(metadata.version());
    metadataWriteNanos.updateProtoBuf(*stats.mutable_metadata_write_nanos());
    filesystemOpsNanos.updateProtoBuf(*stats.mutable_filesystem_ops_nanos());
}


////////// SegmentedLog initialization helper functions //////////


std::vector<SegmentedLog::Segment>
SegmentedLog::readSegmentFilenames()
{
    std::vector<Segment> segments;
    std::vector<std::string> filenames = FS::ls(dir);
    // sorting isn't strictly necessary, but it helps with unit tests
    std::sort(filenames.begin(), filenames.end());
    for (auto it = filenames.begin(); it != filenames.end(); ++it) {
        const std::string& filename = *it;
        if (filename == "metadata1" ||
            filename == "metadata2") {
            continue;
        }
        Segment segment;
        segment.filename = filename;
        segment.bytes = 0;
        { // Closed segment: xxx-yyy
            uint64_t startIndex = 1;
            uint64_t endIndex = 0;
            unsigned bytesConsumed;
            int matched = sscanf(filename.c_str(),
                                 CLOSED_SEGMENT_FORMAT "%n",
                                 &startIndex, &endIndex,
                                 &bytesConsumed);
            if (matched == 2 && bytesConsumed == filename.length()) {
                segment.isOpen = false;
                segment.startIndex = startIndex;
                segment.endIndex = endIndex;
                segments.push_back(segment);
                continue;
            }
        }

        { // Open segment: open-xxx
            uint64_t counter;
            unsigned bytesConsumed;
            int matched = sscanf(filename.c_str(),
                                 OPEN_SEGMENT_FORMAT "%n",
                                 &counter,
                                 &bytesConsumed);
            if (matched == 1 && bytesConsumed == filename.length()) {
                segment.isOpen = true;
                segment.startIndex = ~0UL;
                segment.endIndex = ~0UL - 1;
                segments.push_back(segment);
                preparedSegments.foundFile(counter);
                continue;
            }
        }

        // Neither
        WARNING("%s doesn't look like a valid segment filename (from %s)",
                filename.c_str(),
                (dir.path + "/" + filename).c_str());
    }
    return segments;
}

bool
SegmentedLog::readMetadata(const std::string& filename,
                           SegmentedLogMetadata::Metadata& metadata,
                           bool quiet) const
{
    std::string error;
    FS::File file = FS::tryOpenFile(dir, filename, O_RDONLY);
    if (file.fd == -1) {
        error = format("Could not open %s/%s: %s",
                       dir.path.c_str(), filename.c_str(), strerror(errno));
    } else {
        FS::FileContents reader(file);
        uint64_t offset = 0;
        error = readProtoFromFile(file, reader, &offset, &metadata);
    }
    if (error.empty()) {
        if (metadata.format_version() > 1) {
            PANIC("The format version found in %s is %lu but this code "
                  "only understands version 1",
                  filename.c_str(),
                  metadata.format_version());
        }
        NOTICE("Read metadata version %lu from %s",
               metadata.version(), filename.c_str());
        return true;
    } else {
        if (!quiet) {
            WARNING("Error reading metadata from %s: %s",
                    filename.c_str(), error.c_str());
        }
        return false;
    }
}

bool
SegmentedLog::loadClosedSegment(Segment& segment, uint64_t logStartIndex)
{
    assert(!segment.isOpen);
    FS::File file = FS::openFile(dir, segment.filename, O_RDWR);
    FS::FileContents reader(file);
    uint64_t offset = 0;

    if (reader.getFileLength() < 1) {
        PANIC("Found completely empty segment file %s (it doesn't even have "
              "a version field)",
              segment.filename.c_str());
    } else {
        uint8_t version = *reader.get<uint8_t>(0, 1);
        offset += 1;
        if (version != 1) {
            PANIC("Segment version read from %s was %u, but this code can "
                  "only read version 1",
                  segment.filename.c_str(),
                  version);
        }
    }

    if (segment.endIndex < logStartIndex) {
        NOTICE("Removing closed segment whose entries are no longer "
               "needed (last index is %lu but log start index is %lu): %s",
               segment.endIndex,
               logStartIndex,
               segment.filename.c_str());
        FS::removeFile(dir, segment.filename);
        FS::fsync(dir);
        return false;
    }

    for (uint64_t index = segment.startIndex;
         index <= segment.endIndex;
         ++index) {
        std::string error;
        if (offset >= reader.getFileLength()) {
            error = "File too short";
        } else {
            segment.entries.emplace_back(offset);
            error = readProtoFromFile(file, reader, &offset,
                                      &segment.entries.back().entry);
        }
        if (!error.empty()) {
            PANIC("Could not read entry %lu in log segment %s "
                  "(offset %lu bytes). This indicates the file was "
                  "somehow corrupted. Error was: %s",
                  index,
                  segment.filename.c_str(),
                  offset,
                  error.c_str());
        }
    }
    if (offset < reader.getFileLength()) {
        WARNING("Found an extra %lu bytes at the end of closed segment "
                "%s. This can happen if the server crashed while "
                "truncating the segment. Truncating these now.",
                reader.getFileLength() - offset,
                segment.filename.c_str());
        // TODO(ongaro): do we want to save these bytes somewhere?
        FS::truncate(file, offset);
        FS::fsync(file);
    }
    segment.bytes = offset;
    totalClosedSegmentBytes += segment.bytes;
    return true;
}

bool
SegmentedLog::loadOpenSegment(Segment& segment, uint64_t logStartIndex)
{
    assert(segment.isOpen);
    FS::File file = FS::openFile(dir, segment.filename, O_RDWR);
    FS::FileContents reader(file);
    uint64_t offset = 0;

    if (reader.getFileLength() < 1) {
        WARNING("Found completely empty segment file %s (it doesn't even have "
                "a version field)",
                segment.filename.c_str());
    } else {
        uint8_t version = *reader.get<uint8_t>(0, 1);
        offset += 1;
        if (version != 1) {
            PANIC("Segment version read from %s was %u, but this code can "
                  "only read version 1",
                  segment.filename.c_str(),
                  version);
        }
    }

    uint64_t lastIndex = 0;
    while (offset < reader.getFileLength()) {
        segment.entries.emplace_back(offset);
        std::string error = readProtoFromFile(
                file,
                reader,
                &offset,
                &segment.entries.back().entry);
        if (!error.empty()) {
            segment.entries.pop_back();
            uint64_t remainingBytes = reader.getFileLength() - offset;
            if (isAllZeros(reader.get(offset, remainingBytes),
                           remainingBytes)) {
                WARNING("Truncating %lu zero bytes at the end of log "
                        "segment %s (%lu bytes into the segment, "
                        "following  entry %lu). This is most likely "
                        "because the server shutdown uncleanly.",
                        remainingBytes,
                        segment.filename.c_str(),
                        offset,
                        lastIndex);
            } else {
                // TODO(ongaro): do we want to save these bytes somewhere?
                WARNING("Could not read entry in log segment %s "
                        "(%lu bytes into the segment, following "
                        "entry %lu), probably because it was being "
                        "written when the server crashed. Discarding the "
                        "remainder of the file (%lu bytes). Error was: %s",
                        segment.filename.c_str(),
                        offset,
                        lastIndex,
                        remainingBytes,
                        error.c_str());
            }
            FS::truncate(file, offset);
            FS::fsync(file);
            break;
        }
        lastIndex = segment.entries.back().entry.index();
    }

    bool remove = false;
    if (segment.entries.empty()) {
        NOTICE("Removing empty segment: %s", segment.filename.c_str());
        remove = true;
    } else if (segment.entries.back().entry.index() < logStartIndex) {
        NOTICE("Removing open segment whose entries are no longer "
               "needed (last index is %lu but log start index is %lu): %s",
               segment.entries.back().entry.index(),
               logStartIndex,
               segment.filename.c_str());
        remove = true;
    }
    if (remove) {
        FS::removeFile(dir, segment.filename);
        FS::fsync(dir);
        return false;
    } else {
        segment.bytes = offset;
        totalClosedSegmentBytes += segment.bytes;
        segment.isOpen = false;
        segment.startIndex = segment.entries.front().entry.index();
        segment.endIndex = segment.entries.back().entry.index();
        std::string newFilename = segment.makeClosedFilename();
        NOTICE("Closing open segment %s, renaming to %s",
                segment.filename.c_str(),
                newFilename.c_str());
        FS::rename(dir, segment.filename,
                   dir, newFilename);
        FS::fsync(dir);
        segment.filename = newFilename;
        return true;
    }
}


////////// SegmentedLog normal operation helper functions //////////


void
SegmentedLog::checkInvariants()
{
    if (!shouldCheckInvariants)
        return;
#if DEBUG
    assert(openSegmentFile.fd >= 0);
    assert(!segmentsByStartIndex.empty());
    assert(logStartIndex >= segmentsByStartIndex.begin()->second.startIndex);
    assert(logStartIndex <= segmentsByStartIndex.begin()->second.endIndex + 1);
    assert(currentSync.get() != NULL);
    uint64_t closedBytes = 0;
    for (auto it = segmentsByStartIndex.begin();
         it != segmentsByStartIndex.end();
         ++it) {
        auto next = it;
        ++next;
        Segment& segment = it->second;
        assert(it->first == segment.startIndex);
        assert(segment.startIndex > 0);
        assert(segment.entries.size() ==
               segment.endIndex + 1 - segment.startIndex);
        uint64_t lastOffset = 0;
        for (uint64_t i = 0; i < segment.entries.size(); ++i) {
            assert(segment.entries.at(i).entry.index() ==
                   segment.startIndex + i);
            if (i == 0)
                assert(segment.entries.at(0).offset == sizeof(SegmentHeader));
            else
                assert(segment.entries.at(i).offset > lastOffset);
            lastOffset = segment.entries.at(i).offset;
        }
        if (next == segmentsByStartIndex.end()) {
            assert(segment.isOpen);
            assert(segment.endIndex >= segment.startIndex - 1);
            assert(Core::StringUtil::startsWith(segment.filename, "open-"));
            assert(segment.bytes >= sizeof(SegmentHeader));
        } else {
            assert(!segment.isOpen);
            assert(segment.endIndex >= segment.startIndex);
            assert(next->second.startIndex == segment.endIndex + 1);
            assert(segment.bytes > sizeof(SegmentHeader));
            closedBytes += segment.bytes;
            assert(segment.filename == segment.makeClosedFilename());
        }
    }
    assert(closedBytes == totalClosedSegmentBytes);
#endif /* DEBUG */
}

void
SegmentedLog::closeSegment()
{
    if (openSegmentFile.fd < 0)
        return;
    Segment& openSegment = getOpenSegment();
    if (openSegment.startIndex > openSegment.endIndex) {
        // Segment is empty; just remove it.
        NOTICE("Removing empty open segment (start index %lu): %s",
               openSegment.startIndex,
               openSegment.filename.c_str());
        openSegmentFile.close();
        FS::removeFile(dir, openSegment.filename);
        FS::fsync(dir);
        segmentsByStartIndex.erase(openSegment.startIndex);
        return;
    }

    // Truncate away any extra 0 bytes at the end from when
    // MAX_SEGMENT_SIZE was allocated, or in the case of truncateSuffix,
    // truncate away actual entries that are no longer desired.
    FS::truncate(openSegmentFile, openSegment.bytes);
    FS::fsync(openSegmentFile);
    openSegmentFile.close();

    // Rename the file.
    std::string newFilename = openSegment.makeClosedFilename();
    NOTICE("Closing segment (was %s, renaming to %s)",
           openSegment.filename.c_str(),
           newFilename.c_str());
    FS::rename(dir, openSegment.filename,
               dir, newFilename);
    FS::fsync(dir);
    openSegment.filename = newFilename;

    openSegment.isOpen = false;
    totalClosedSegmentBytes += openSegment.bytes;
}

SegmentedLog::Segment&
SegmentedLog::getOpenSegment()
{
    assert(!segmentsByStartIndex.empty());
    return segmentsByStartIndex.rbegin()->second;
}

const SegmentedLog::Segment&
SegmentedLog::getOpenSegment() const
{
    assert(!segmentsByStartIndex.empty());
    return segmentsByStartIndex.rbegin()->second;
}

void
SegmentedLog::openNewSegment()
{
    assert(openSegmentFile.fd < 0);
    assert(segmentsByStartIndex.empty() ||
           !segmentsByStartIndex.rbegin()->second.isOpen);

    Segment newSegment;
    newSegment.isOpen = true;
    newSegment.startIndex = getLastLogIndex() + 1;
    newSegment.endIndex = newSegment.startIndex - 1;
    newSegment.bytes = sizeof(SegmentHeader);
    // This can throw ThreadInterruptedException, but it shouldn't ever, since
    // this class shouldn't have been destroyed yet.
    auto s = preparedSegments.waitForOpenSegment();
    newSegment.filename = s.first;
    openSegmentFile = std::move(s.second);
    segmentsByStartIndex.insert({newSegment.startIndex, newSegment});
}

std::string
SegmentedLog::readProtoFromFile(const FS::File& file,
                                FS::FileContents& reader,
                                uint64_t* offset,
                                google::protobuf::Message* out) const
{
    uint64_t loffset = *offset;
    char checksum[Core::Checksum::MAX_LENGTH];
    uint64_t bytesRead = reader.copyPartial(loffset, checksum,
                                            sizeof(checksum));
    uint32_t checksumBytes = Core::Checksum::length(checksum,
                                                    uint32_t(bytesRead));
    if (checksumBytes == 0)
        return format("Missing checksum in file %s", file.path.c_str());
    loffset += checksumBytes;

    uint64_t dataLen;
    if (reader.copyPartial(loffset, &dataLen, sizeof(dataLen)) <
        sizeof(dataLen)) {
        return format("Record length truncated in file %s", file.path.c_str());
    }
    dataLen = be64toh(dataLen);
    if (reader.getFileLength() < loffset + sizeof(dataLen) + dataLen) {
        return format("ProtoBuf truncated in file %s", file.path.c_str());
    }

    const void* checksumCoverage = reader.get(loffset,
                                              sizeof(dataLen) + dataLen);
    std::string error = Core::Checksum::verify(checksum, checksumCoverage,
                                               sizeof(dataLen) + dataLen);
    if (!error.empty()) {
        return format("Checksum verification failure on %s: %s",
                      file.path.c_str(), error.c_str());
    }
    loffset += sizeof(dataLen);
    const void* data = reader.get(loffset, dataLen);
    loffset += dataLen;

    switch (encoding) {
        case SegmentedLog::Encoding::BINARY: {
            Core::Buffer contents(const_cast<void*>(data),
                                  dataLen,
                                  NULL);
            if (!Core::ProtoBuf::parse(contents, *out)) {
                return format("Failed to parse protobuf in %s",
                              file.path.c_str());
            }
            break;
        }
        case SegmentedLog::Encoding::TEXT: {
            std::string contents(static_cast<const char*>(data), dataLen);
            Core::ProtoBuf::Internal::fromString(contents, *out);
            break;
        }
    }
    *offset = loffset;
    return "";
}

Core::Buffer
SegmentedLog::serializeProto(const google::protobuf::Message& in) const
{
    // TODO(ongaro): can the intermediate buffer be avoided?
    const void* data = NULL;
    uint64_t len = 0;
    Core::Buffer binaryContents;
    std::string asciiContents;
    switch (encoding) {
        case SegmentedLog::Encoding::BINARY: {
            Core::ProtoBuf::serialize(in, binaryContents);
            data = binaryContents.getData();
            len = binaryContents.getLength();
            break;
        }
        case SegmentedLog::Encoding::TEXT: {
            asciiContents = Core::ProtoBuf::dumpString(in);
            data = asciiContents.data();
            len = asciiContents.length();
            break;
        }
    }
    uint64_t netLen = htobe64(len);
    char checksum[Core::Checksum::MAX_LENGTH];
    uint32_t checksumLen = Core::Checksum::calculate(
        checksumAlgorithm.c_str(), {
            {&netLen, sizeof(netLen)},
            {data, len},
        },
        checksum);

    uint64_t totalLen = checksumLen + sizeof(netLen) + len;
    char* buf = new char[totalLen];
    Core::Buffer record(
        buf,
        totalLen,
        Core::Buffer::deleteArrayFn<char>);
    Core::Util::memcpy(buf, {
        {checksum, checksumLen},
        {&netLen, sizeof(netLen)},
        {data, len},
    });
    return record;
}


////////// SegmentedLog segment preparer thread functions //////////

std::pair<std::string, FS::File>
SegmentedLog::prepareNewSegment(uint64_t id)
{
    TimePoint start = Clock::now();

    std::string filename = format(OPEN_SEGMENT_FORMAT, id);
    FS::File file = FS::openFile(dir, filename,
                                 O_CREAT|O_EXCL|O_RDWR);
    FS::allocate(file, 0, MAX_SEGMENT_SIZE);
    SegmentHeader header;
    header.version = 1;
    ssize_t written = FS::write(file.fd,
                                &header,
                                sizeof(header));
    if (written == -1) {
        PANIC("Failed to write header to %s: %s",
              file.path.c_str(), strerror(errno));
    }
    FS::fsync(file);
    FS::fsync(dir);

    TimePoint end = Clock::now();
    std::chrono::nanoseconds elapsed = end - start;
    // TODO(ongaro): record elapsed times into RollingStat in a thread-safe way
    if (elapsed > diskWriteDurationThreshold) {
        WARNING("Preparing open segment file took longer than expected (%s)",
                Core::StringUtil::toString(elapsed).c_str());
    }
    return {std::move(filename), std::move(file)};
}


void
SegmentedLog::segmentPreparerMain()
{
    Core::ThreadId::setName("SegmentPreparer");
    while (true) {
        uint64_t fileId = 0;
        try {
            fileId = preparedSegments.waitForDemand();
        } catch (const Core::Util::ThreadInterruptedException&) {
            VERBOSE("Exiting");
            break;
        }
        preparedSegments.submitOpenSegment(
            prepareNewSegment(fileId));
    }
}

} // namespace LogCabin::Storage
} // namespace LogCabin
