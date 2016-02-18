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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>

#include "Core/Config.h"
#include "Core/ProtoBuf.h"
#include "Core/STLUtil.h"
#include "Core/Util.h"
#include "Storage/FilesystemUtil.h"
#include "Storage/Layout.h"
#include "Storage/SegmentedLog.h"
#include "include/LogCabin/Debug.h"

namespace LogCabin {
namespace Storage {
namespace {

namespace FS = FilesystemUtil;
using Core::STLUtil::sorted;

std::vector<SegmentedLog::Sync::Op::OpCode>
extractOpCodes(const SegmentedLog::Sync& sync)
{
    std::vector<SegmentedLog::Sync::Op::OpCode> ret;
    for (auto it = sync.ops.begin(); it != sync.ops.end(); ++it)
        ret.push_back(it->opCode);
    return ret;
}

TEST(StorageSegmentedLogSyncTest, optimize)
{
    typedef SegmentedLog::Sync::Op Op;
    SegmentedLog::Sync sync(0, std::chrono::nanoseconds(1));

    sync.optimize(); // hopefully no out of bounds issues
    EXPECT_EQ(0U, sync.ops.size());

    // easy optimization case
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    sync.optimize();
    EXPECT_EQ((std::vector<Op::OpCode> {
                   Op::WRITE,
                   Op::NOOP,
                   Op::WRITE,
                   Op::FDATASYNC,
               }), extractOpCodes(sync));

    // a few more
    sync.ops.clear();
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    sync.optimize();
    EXPECT_EQ((std::vector<Op::OpCode> {
                   Op::WRITE,
                   Op::NOOP,
                   Op::WRITE,
                   Op::NOOP,
                   Op::WRITE,
                   Op::NOOP,
                   Op::WRITE,
                   Op::FDATASYNC,
               }), extractOpCodes(sync));

    // trickier cases: differing fds
    sync.ops.clear();
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(31, Op::FDATASYNC);
    sync.ops.emplace_back(32, Op::WRITE);
    sync.ops.emplace_back(31, Op::FDATASYNC);
    sync.ops.emplace_back(32, Op::WRITE);
    sync.ops.emplace_back(32, Op::FDATASYNC);
    EXPECT_EQ((std::vector<Op::OpCode> {
                   Op::WRITE,
                   Op::FDATASYNC,
                   Op::WRITE,
                   Op::FDATASYNC,
                   Op::WRITE,
                   Op::FDATASYNC,
                   Op::WRITE,
                   Op::FDATASYNC,
               }), extractOpCodes(sync));

    // trickier cases: differing ops
    sync.ops.clear();
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::WRITE);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    sync.ops.emplace_back(30, Op::FSYNC);
    sync.ops.emplace_back(30, Op::FDATASYNC);
    EXPECT_EQ((std::vector<Op::OpCode> {
                   Op::WRITE,
                   Op::WRITE,
                   Op::FDATASYNC,
                   Op::FSYNC,
                   Op::FDATASYNC,
               }), extractOpCodes(sync));

    sync.completed = true;
}

// One thing to keep in mind for these tests is truncatePrefix. Calling that
// basically affects every other method, so every test should include
// a call to truncatePrefix.

class StorageSegmentedLogTest : public ::testing::Test {
    StorageSegmentedLogTest()
        : config()
        , layout()
        , log()
        , sampleEntry()
        , closedSegment()
        , openSegment()
    {
        config.set<uint64_t>("storageSegmentBytes", 1024);
        config.set<uint64_t>("storageOpenSegments", 1);
        config.set<bool>("unittest-quiet", true);
        config.set<bool>("storageDebug", true);
        layout.initTemporary();

        sampleEntry.set_term(40);
        sampleEntry.set_data("foo");
        sampleEntry.set_cluster_time(1);

        closedSegment.isOpen = false;
        closedSegment.startIndex = 3;
        closedSegment.endIndex = 4009;
        closedSegment.filename = closedSegment.makeClosedFilename();

        openSegment.isOpen = true;
        openSegment.filename = "open-90";

        construct();
    }
    ~StorageSegmentedLogTest()
    {
        log.reset();
    }
    void construct() {
        log.reset(); // shut down existing /before/ constructing new
        log.reset(new SegmentedLog(layout.logDir,
                                   SegmentedLog::Encoding::TEXT,
                                   config));
    }
    void sync() {
        std::unique_ptr<Log::Sync> sync = log->takeSync();
        sync->wait();
        log->syncComplete(std::move(sync));
    }

    void setUpThreeSegments() {
        log->truncatePrefix(3);
        log->append({&sampleEntry, &sampleEntry}); // index 3-4
        sync();
        log->closeSegment();
        log->openNewSegment();
        log->append({&sampleEntry, &sampleEntry}); // index 5-6
        sync();
        log->closeSegment();
        log->openNewSegment();
        log->append({&sampleEntry, &sampleEntry}); // index 7-8
        sync();
        FS::File logDir = FS::dup(log->dir);
        log.reset();
        EXPECT_EQ((std::vector<std::string> {
                        "00000000000000000003-00000000000000000004",
                        "00000000000000000005-00000000000000000006",
                        "00000000000000000007-00000000000000000008",
                        "metadata1",
                        "metadata2",
                   }),
                  sorted(FS::ls(logDir)));
    }

    void readProtoFromFileHelper() {
        uint64_t offset = 5;
        SegmentedLogMetadata::Metadata metadata;
        FS::File file = FS::openFile(log->dir, "f", O_CREAT|O_RDWR);
        uint64_t size;
        {
            log->updateMetadata();
            Core::Buffer record = log->serializeProto(log->metadata);
            EXPECT_EQ(5, FS::write(file.fd, "abcde", 5));
            EXPECT_LE(0, FS::write(file.fd,
                                   record.getData(),
                                   record.getLength()));
            size = FS::getSize(file);
        }

        { // make sure there's no error now
            FS::FileContents contents(file);
            EXPECT_EQ("", log->readProtoFromFile(file, contents,
                                                 &offset, &metadata));
            EXPECT_EQ(size, offset);
            offset = 5;
        }

        // invert each byte and make sure there's an error
        uint8_t* map = static_cast<uint8_t*>(
            mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, file.fd, 0));
        for (uint64_t i = 5; i < size; ++i) {
            map[i] = uint8_t(~map[i]);
            FS::FileContents contents(file);
            std::string error = log->readProtoFromFile(file, contents,
                                                       &offset, &metadata);
            EXPECT_FALSE(error.empty());
            //WARNING("byte flipped=%lu error: %s", i, error.c_str());
            EXPECT_EQ(5U, offset);
            offset = 5;
            map[i] = uint8_t(~map[i]);
        }

        { // make sure there's no error now
            FS::FileContents contents(file);
            EXPECT_EQ("", log->readProtoFromFile(file, contents,
                                                 &offset, &metadata));
            size = FS::getSize(file);
            EXPECT_EQ(size, offset);
            offset = 5;
        }

        // make sure every truncation is an error
        do {
            --size;
            FS::truncate(file, size);
            FS::FileContents contents(file);
            std::string error = log->readProtoFromFile(file, contents,
                                                       &offset, &metadata);
            EXPECT_FALSE(error.empty());
            //WARNING("length=%lu error: %s", size, error.c_str());
            EXPECT_EQ(5U, offset);
            offset = 5;
        } while (size > 5);
    }

    void writeSegmentHeader(FS::File& file, uint8_t version = 1) {
        SegmentedLog::SegmentHeader header;
        header.version = version;
        EXPECT_LE(0, FS::write(file.fd, &header, sizeof(header)))
            << strerror(errno);
    }

    Core::Config config;
    Storage::Layout layout;
    std::unique_ptr<SegmentedLog> log;
    SegmentedLog::Entry sampleEntry;
    SegmentedLog::Segment closedSegment;
    SegmentedLog::Segment openSegment;
};

TEST_F(StorageSegmentedLogTest, basic_blackbox)
{
    std::pair<uint64_t, uint64_t> range = log->append({&sampleEntry});
    EXPECT_EQ(1U, range.first);
    EXPECT_EQ(1U, range.second);
    Log::Entry entry = log->getEntry(1);
    EXPECT_EQ(40U, entry.term());
    EXPECT_EQ("foo", entry.data());
    sync();
}

TEST_F(StorageSegmentedLogTest, append_blackbox)
{
    std::pair<uint64_t, uint64_t> range = log->append({&sampleEntry});
    EXPECT_EQ(1U, range.first);
    EXPECT_EQ(1U, range.second);
    log->truncatePrefix(10);
    range = log->append({&sampleEntry, &sampleEntry});
    EXPECT_EQ(10U, range.first);
    EXPECT_EQ(11U, range.second);
    EXPECT_EQ(10U, log->getLogStartIndex());
    EXPECT_EQ(11U, log->getLastLogIndex());
    sync();
}

TEST_F(StorageSegmentedLogTest, getEntry_blackbox)
{
    log->append({&sampleEntry});
    Log::Entry entry = log->getEntry(1);
    EXPECT_EQ(40U, entry.term());
    EXPECT_EQ("foo", entry.data());
    EXPECT_DEATH(log->getEntry(0), "outside");
    EXPECT_DEATH(log->getEntry(2), "outside");

    sampleEntry.set_data("bar");
    log->append({&sampleEntry});
    log->truncatePrefix(2);
    EXPECT_DEATH(log->getEntry(1), "outside");
    log->append({&sampleEntry});
    Log::Entry entry2 = log->getEntry(2);
    EXPECT_EQ("bar", entry2.data());
    sync();
}

TEST_F(StorageSegmentedLogTest, getLogStartIndex_blackbox)
{
    EXPECT_EQ(1U, log->getLogStartIndex());
    log->truncatePrefix(200);
    log->truncatePrefix(100);
    EXPECT_EQ(200U, log->getLogStartIndex());
    sync();
}

TEST_F(StorageSegmentedLogTest, getLastLogIndex_blackbox)
{
    EXPECT_EQ(0U, log->getLastLogIndex());
    log->append({&sampleEntry});
    log->append({&sampleEntry});
    EXPECT_EQ(2U, log->getLastLogIndex());

    log->truncatePrefix(2);
    EXPECT_EQ(2U, log->getLastLogIndex());
    sync();
}

TEST_F(StorageSegmentedLogTest, getSizeBytes_blackbox)
{
    const uint64_t SLOP = 100;
    const uint64_t DATALEN = 1000;
    config.set<uint64_t>("storageSegmentBytes", DATALEN * 2);
    construct();
    EXPECT_GT(SLOP, log->getSizeBytes());
    sampleEntry.set_index(1);
    sampleEntry.set_data(std::string(DATALEN, 'c'));
    log->append({&sampleEntry});
    EXPECT_LE(DATALEN, log->getSizeBytes());
    EXPECT_GT(DATALEN + 2 * SLOP, log->getSizeBytes());
    sampleEntry.set_index(2);
    log->append({&sampleEntry});
    EXPECT_LE(DATALEN * 2, log->getSizeBytes());
    EXPECT_GT(DATALEN * 2 + 3 * SLOP, log->getSizeBytes());
    sampleEntry.set_index(3);
    log->append({&sampleEntry});
    EXPECT_LE(DATALEN * 3, log->getSizeBytes());
    EXPECT_GT(DATALEN * 3 + 4 * SLOP, log->getSizeBytes());
    sync();
}

TEST_F(StorageSegmentedLogTest, truncatePrefix_blackbox)
{
    EXPECT_EQ(1U, log->getLogStartIndex());
    log->truncatePrefix(0);
    EXPECT_EQ(1U, log->getLogStartIndex());
    log->truncatePrefix(1);
    EXPECT_EQ(1U, log->getLogStartIndex());

    // case 1: entries is empty
    log->truncatePrefix(500);
    EXPECT_EQ(500U, log->getLogStartIndex());
    EXPECT_EQ(499U, log->getLastLogIndex());

    // case 2: entries has fewer elements than truncated
    log->append({&sampleEntry});
    log->truncatePrefix(502);
    EXPECT_EQ(502U, log->getLogStartIndex());
    EXPECT_EQ(501U, log->getLastLogIndex());

    // case 3: entries has exactly the elements truncated
    log->append({&sampleEntry});
    log->append({&sampleEntry});
    log->truncatePrefix(504);
    EXPECT_EQ(504U, log->getLogStartIndex());
    EXPECT_EQ(503U, log->getLastLogIndex());

    // case 4: entries has more elements than truncated
    log->append({&sampleEntry});
    log->append({&sampleEntry});
    sampleEntry.set_data("bar");
    log->append({&sampleEntry});
    log->truncatePrefix(506);
    EXPECT_EQ(506U, log->getLogStartIndex());
    EXPECT_EQ(506U, log->getLastLogIndex());
    EXPECT_EQ("bar", log->getEntry(506).data());

    // make sure truncating to an earlier id has no effect
    log->truncatePrefix(400);
    EXPECT_EQ(506U, log->getLogStartIndex());
    EXPECT_EQ(506U, log->getLastLogIndex());
    sync();
}

TEST_F(StorageSegmentedLogTest, truncateSuffix_blackbox)
{
    log->truncateSuffix(0);
    log->truncateSuffix(10);
    EXPECT_EQ(0U, log->getLastLogIndex());
    log->append({&sampleEntry});
    log->append({&sampleEntry});
    sync();
    log->truncateSuffix(10);
    EXPECT_EQ(2U, log->getLastLogIndex());
    log->truncateSuffix(2);
    EXPECT_EQ(2U, log->getLastLogIndex());
    log->truncateSuffix(1);
    EXPECT_EQ(1U, log->getLastLogIndex());
    log->truncateSuffix(0);
    EXPECT_EQ(0U, log->getLastLogIndex());


    log->truncatePrefix(10);
    log->append({&sampleEntry});
    EXPECT_EQ(10U, log->getLastLogIndex());
    sync();
    log->truncateSuffix(10);
    EXPECT_EQ(10U, log->getLastLogIndex());
    log->truncateSuffix(8);
    EXPECT_EQ(9U, log->getLastLogIndex());
    log->append({&sampleEntry});
    EXPECT_EQ(10U, log->getLastLogIndex());
    sync();
}

TEST_F(StorageSegmentedLogTest, constructor_metadata)
{
    SegmentedLogMetadata::Metadata m1;
    SegmentedLogMetadata::Metadata m2;

    // metadata1 is bad,  metadata2 is bad
    EXPECT_EQ(true, log->readMetadata("metadata1", m1, false));
    EXPECT_EQ(true, log->readMetadata("metadata2", m2, false));
    EXPECT_EQ("version: 1 "
              "format_version: 1 "
              "entries_start: 1", m1);
    EXPECT_EQ("version: 2 "
              "format_version: 1 "
              "entries_start: 1", m2);

    // metadata1 is bad,  metadata2 is good
    log->logStartIndex = 3;
    log->updateMetadata(); // v3, v2
    log->updateMetadata(); // v3, v4
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    FS::removeFile(logDir, "metadata1");
    construct();
    EXPECT_EQ(3U, log->logStartIndex);
    EXPECT_EQ(true, log->readMetadata("metadata1", m1, false));
    EXPECT_EQ(true, log->readMetadata("metadata2", m2, false));
    EXPECT_EQ("version: 5 "
              "format_version: 1 "
              "entries_start: 3", m1);
    EXPECT_EQ("version: 6 "
              "format_version: 1 "
              "entries_start: 3", m2);

    // metadata1 is good, metadata2 is bad
    log->logStartIndex = 6;
    log->updateMetadata(); // v7, v6
    log.reset();
    FS::removeFile(logDir, "metadata2");
    construct();
    EXPECT_EQ(6U, log->logStartIndex);

    // metadata1 is v1,   metadata2 is v2
    log->logStartIndex = 9;
    log->updateMetadata();
    construct();
    EXPECT_EQ(9U, log->logStartIndex);

    // metadata1 is v2,   metadata2 is v1
    log->logStartIndex = 12;
    log->updateMetadata();
    construct();
    EXPECT_EQ(12U, log->logStartIndex);

    // no metadata but segments exist
    log.reset();
    FS::removeFile(logDir, "metadata1");
    FS::removeFile(logDir, "metadata2");
    FS::openFile(logDir, "open-1", O_CREAT);
    EXPECT_DEATH(construct(),
                 "No readable metadata file but found segments");
}

TEST_F(StorageSegmentedLogTest, constructor_segmentsByStartIndex)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry});
    sync();
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    FS::File file;
    file = FS::openFile(logDir, "open-1", O_CREAT|O_WRONLY);
    writeSegmentHeader(file);
    file = FS::openFile(logDir, "open-2", O_CREAT|O_WRONLY);
    writeSegmentHeader(file);
    construct();
    EXPECT_EQ(2U, log->segmentsByStartIndex.size());
    EXPECT_EQ("00000000000000000003-00000000000000000004",
              log->segmentsByStartIndex.at(3).filename);
    EXPECT_EQ("open-3",
              log->segmentsByStartIndex.at(5).filename);
}

TEST_F(StorageSegmentedLogTest, constructor_nogap_segmentMissing)
{
    FS::File logDir = FS::dup(log->dir);
    setUpThreeSegments();
    FS::removeFile(logDir, "00000000000000000005-00000000000000000006");
    EXPECT_DEATH(construct(),
                 "Did not find segment containing entries 5 to 6");
}

TEST_F(StorageSegmentedLogTest, constructor_nogap_entryMissing)
{
    FS::File logDir = FS::dup(log->dir);
    setUpThreeSegments();
    FS::rename(logDir, "00000000000000000005-00000000000000000006",
               logDir, "00000000000000000005-00000000000000000005");
    EXPECT_DEATH(construct(),
                 "Did not find segment containing entries 6 to 6");
}

TEST_F(StorageSegmentedLogTest, constructor_nodup_sameStartIndex)
{
    FS::File logDir = FS::dup(log->dir);
    setUpThreeSegments();
    {
        FS::File oldFile =
            FS::openFile(logDir,
                         "00000000000000000005-00000000000000000006",
                         O_RDONLY);
        FS::FileContents contents(oldFile);
        FS::File newFile =
            FS::openFile(logDir,
                         "00000000000000000005-00000000000000000005",
                         O_CREAT|O_RDWR);
        EXPECT_LT(0, FS::write(newFile.fd,
                               contents.get(0, contents.getFileLength()),
                               contents.getFileLength()));
    }
    EXPECT_DEATH(construct(),
                 "Two segments contain entry 5");
}

TEST_F(StorageSegmentedLogTest, constructor_nodup_differentStartIndex)
{
    FS::File logDir = FS::dup(log->dir);
    setUpThreeSegments();
    {
        FS::File oldFile =
            FS::openFile(logDir,
                         "00000000000000000005-00000000000000000006",
                         O_RDONLY);
        FS::FileContents contents(oldFile);
        FS::File newFile =
            FS::openFile(logDir,
                         "00000000000000000006-00000000000000000006",
                         O_CREAT|O_RDWR);
        // copy exactly the bytes for entry 6
        uint64_t len = (contents.getFileLength() -
                        sizeof(SegmentedLog::SegmentHeader)) / 2;
        uint64_t start = sizeof(SegmentedLog::SegmentHeader) + len;
        writeSegmentHeader(newFile);
        EXPECT_LT(0, FS::write(newFile.fd,
                               contents.get(start, len),
                               len));
    }
    // regex follows, filename is 0000...6-0000...6
    EXPECT_DEATH(construct(),
                 "Segment 0+6-0+6 contains duplicate entries 6 to 6");
}

TEST_F(StorageSegmentedLogTest, destructor_cleanshutdown)
{
    construct();
}

// This depends on the exact size of sampleEntry's record, and it may need to
// be adjusted if the record format changes.
TEST_F(StorageSegmentedLogTest, append_rollover)
{
    log->truncatePrefix(3);
    std::vector<const Log::Entry*> entries;
    for (uint64_t i = 3; i <= 19; ++i)
        entries.push_back(&sampleEntry);
    EXPECT_EQ((std::pair<uint64_t, uint64_t>{3, 19}),
              log->append(entries));
    EXPECT_EQ((std::vector<uint64_t> { 3, 17 }),
              Core::STLUtil::getKeys(log->segmentsByStartIndex))
        << "This test may fail when record sizes change.";
    EXPECT_EQ(sizeof(SegmentedLog::SegmentHeader),
              log->segmentsByStartIndex.at(17).entries.at(0).offset);
    EXPECT_EQ(19U, log->currentSync->lastIndex);
    sync();
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    EXPECT_EQ((std::vector<std::string> {
                    "00000000000000000003-00000000000000000016",
                    "00000000000000000017-00000000000000000019",
                    "metadata1",
                    "metadata2",
               }),
              sorted(FS::ls(logDir)));
    EXPECT_GE(1024U, getSize(FS::openFile(
                                logDir,
                                "00000000000000000003-00000000000000000016",
                                O_RDONLY)));
    construct(); // extra sanity checks
}

TEST_F(StorageSegmentedLogTest, append_largerThanMaxSegmentSize)
{
    SegmentedLog::Entry bigEntry = sampleEntry;
    bigEntry.set_data(Core::StringUtil::format("%01000u", 1));
    LogCabin::Core::Debug::setLogPolicy({ // expect warnings
        {"Storage/SegmentedLog", "ERROR"}
    });
    EXPECT_EQ((std::pair<uint64_t, uint64_t>{1, 5}),
              log->append({&bigEntry,
                           &sampleEntry,
                           &sampleEntry,
                           &bigEntry,
                           &sampleEntry}));
    LogCabin::Core::Debug::setLogPolicy({
        {"", "WARNING"}
    });
    EXPECT_EQ((std::vector<uint64_t> { 1, 2, 4, 5 }),
              Core::STLUtil::getKeys(log->segmentsByStartIndex));
    EXPECT_EQ(sizeof(SegmentedLog::SegmentHeader),
              log->segmentsByStartIndex.at(4).entries.at(0).offset);
    EXPECT_EQ(5U, log->currentSync->lastIndex);
    sync();
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    EXPECT_EQ((std::vector<std::string> {
                    "00000000000000000001-00000000000000000001",
                    "00000000000000000002-00000000000000000003",
                    "00000000000000000004-00000000000000000004",
                    "00000000000000000005-00000000000000000005",
                    "metadata1",
                    "metadata2",
               }),
              sorted(FS::ls(logDir)));
    construct(); // extra sanity checks
}

// getEntry, getLogStartIndex, getLastLogIndex tested sufficiently by blackbox
// tests

// getSizeBytes and takeSync are trivial

TEST_F(StorageSegmentedLogTest, truncatePrefix_noSegments)
{
    log->truncatePrefix(7);
    log->truncatePrefix(6);
    EXPECT_EQ(7U, log->getLogStartIndex());
    sync();
    construct();
    EXPECT_EQ(7U, log->getLogStartIndex());
}

TEST_F(StorageSegmentedLogTest, truncatePrefix_someSegments)
{
    log->append({&sampleEntry, &sampleEntry}); // index 1-2
    sync();
    log->closeSegment();
    log->openNewSegment();
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->closeSegment();
    log->openNewSegment();
    log->append({&sampleEntry, &sampleEntry}); // index 5-6
    log->truncatePrefix(4);
    log->truncatePrefix(3);
    sync();
    EXPECT_EQ((std::vector<uint64_t> { 3, 5 }),
              Core::STLUtil::getKeys(log->segmentsByStartIndex));
    EXPECT_EQ(6U, log->currentSync->lastIndex);
    sync();
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    EXPECT_EQ((std::vector<std::string> {
                    "00000000000000000003-00000000000000000004",
                    "00000000000000000005-00000000000000000006",
                    "metadata1",
                    "metadata2",
               }),
              sorted(FS::ls(logDir)));
}

TEST_F(StorageSegmentedLogTest, truncatePrefix_allSegments)
{
    log->append({&sampleEntry, &sampleEntry}); // index 1-2
    sync();
    log->closeSegment();
    log->openNewSegment();
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->closeSegment();
    log->openNewSegment();
    log->append({&sampleEntry, &sampleEntry}); // index 5-6
    log->truncatePrefix(7);
    log->truncatePrefix(6);
    sync();
    EXPECT_EQ((std::vector<uint64_t> { 7 }),
              Core::STLUtil::getKeys(log->segmentsByStartIndex));
    EXPECT_EQ(6U, log->currentSync->lastIndex);
    sync();
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    EXPECT_EQ((std::vector<std::string> {
                    "metadata1",
                    "metadata2",
               }),
              sorted(FS::ls(logDir)));
}

TEST_F(StorageSegmentedLogTest, truncateSuffix_noop)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->truncateSuffix(4);
    EXPECT_EQ(4U, log->getLastLogIndex());
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    EXPECT_EQ((std::vector<std::string> {
                    "00000000000000000003-00000000000000000004",
                    "metadata1",
                    "metadata2",
               }),
              sorted(FS::ls(logDir)));
    construct();
    EXPECT_EQ(4U, log->getLastLogIndex());
}

TEST_F(StorageSegmentedLogTest, truncateSuffix_openSegment_partial)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->truncateSuffix(3);
    EXPECT_EQ((std::vector<uint64_t> { 3, 4 }),
              Core::STLUtil::getKeys(log->segmentsByStartIndex));
    EXPECT_EQ(1U, log->segmentsByStartIndex.at(3).entries.size());
    EXPECT_EQ(3U, log->segmentsByStartIndex.at(3).endIndex);
    EXPECT_EQ(0U, log->segmentsByStartIndex.at(4).entries.size());
    EXPECT_EQ(3U, log->segmentsByStartIndex.at(4).endIndex);
    EXPECT_EQ(sizeof(SegmentedLog::SegmentHeader),
              log->segmentsByStartIndex.at(4).bytes);
    EXPECT_EQ(3U, log->getLastLogIndex());
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    EXPECT_EQ((std::vector<std::string> {
                    "00000000000000000003-00000000000000000003",
                    "metadata1",
                    "metadata2",
               }),
              sorted(FS::ls(logDir)));
    construct();
    EXPECT_EQ(3U, log->getLastLogIndex());
}

TEST_F(StorageSegmentedLogTest, truncateSuffix_openSegment_full)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->closeSegment();
    log->openNewSegment();
    log->append({&sampleEntry, &sampleEntry}); // index 5-6
    sync();
    log->truncateSuffix(4);
    EXPECT_EQ((std::vector<uint64_t> { 3, 5 }),
              Core::STLUtil::getKeys(log->segmentsByStartIndex));
    EXPECT_EQ(2U, log->segmentsByStartIndex.at(3).entries.size());
    EXPECT_EQ(4U, log->segmentsByStartIndex.at(3).endIndex);
    EXPECT_EQ(0U, log->segmentsByStartIndex.at(5).entries.size());
    EXPECT_EQ(4U, log->segmentsByStartIndex.at(5).endIndex);
    EXPECT_EQ(sizeof(SegmentedLog::SegmentHeader),
              log->segmentsByStartIndex.at(5).bytes);
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    EXPECT_EQ((std::vector<std::string> {
                    "00000000000000000003-00000000000000000004",
                    "metadata1",
                    "metadata2",
               }),
              sorted(FS::ls(logDir)));
    construct();
    EXPECT_EQ(4U, log->getLastLogIndex());
}

// remove the open segment, a full closed segment, and part of a closed segment
TEST_F(StorageSegmentedLogTest, truncateSuffix_closedSegments)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->closeSegment();
    log->openNewSegment();
    log->append({&sampleEntry, &sampleEntry}); // index 5-6
    sync();
    log->append({&sampleEntry, &sampleEntry}); // index 7-8
    sync();
    log->truncateSuffix(3);
    EXPECT_EQ((std::vector<uint64_t> { 3, 4 }),
              Core::STLUtil::getKeys(log->segmentsByStartIndex));
    EXPECT_EQ(1U, log->segmentsByStartIndex.at(3).entries.size());
    EXPECT_EQ(3U, log->segmentsByStartIndex.at(3).endIndex);
    EXPECT_EQ(0U, log->segmentsByStartIndex.at(4).entries.size());
    EXPECT_EQ(3U, log->segmentsByStartIndex.at(4).endIndex);
    EXPECT_EQ(sizeof(SegmentedLog::SegmentHeader),
              log->segmentsByStartIndex.at(4).bytes);
    FS::File logDir = FS::dup(log->dir);
    log.reset();
    EXPECT_EQ((std::vector<std::string> {
                    "00000000000000000003-00000000000000000003",
                    "metadata1",
                    "metadata2",
               }),
              sorted(FS::ls(logDir)));
    construct();
    EXPECT_EQ(3U, log->getLastLogIndex());
}

// updateMetadata tested pretty well in constructor tests already

TEST_F(StorageSegmentedLogTest, readSegmentFilenames)
{
    log->closeSegment();
    log->preparedSegments.exit();
    log->segmentPreparer.join();
    auto prepared = log->preparedSegments.releaseAll();
    while (!prepared.empty()) {
        std::string filename = prepared.front().first;
        FS::removeFile(log->dir, filename);
        prepared.pop_front();
    }

    std::vector<SegmentedLog::Segment> segments = log->readSegmentFilenames();
    EXPECT_EQ(0U, segments.size());

    FS::openFile(log->dir, "open-1", O_CREAT);
    FS::openFile(log->dir, "open-3", O_CREAT);
    FS::openFile(log->dir, "open-500", O_CREAT);
    FS::openFile(log->dir, "00000000000000000003-00000000000000004009",
                 O_CREAT);
    FS::openFile(log->dir, "metadata1", O_CREAT);
    FS::openFile(log->dir, "metadata2", O_CREAT);
    segments = log->readSegmentFilenames();
    EXPECT_EQ(4U, segments.size());

    EXPECT_EQ("00000000000000000003-00000000000000004009",
              segments.at(0).filename);
    EXPECT_FALSE(segments.at(0).isOpen);
    EXPECT_EQ(segments.at(0).filename,
              segments.at(0).makeClosedFilename());
    EXPECT_EQ(0U, segments.at(0).bytes);
    EXPECT_EQ(0U, segments.at(0).entries.size());

    EXPECT_EQ("open-1",
              segments.at(1).filename);
    EXPECT_TRUE(segments.at(1).isOpen);
    EXPECT_EQ(~0UL, segments.at(1).startIndex);
    EXPECT_EQ(~0UL - 1, segments.at(1).endIndex);
    EXPECT_EQ(0U, segments.at(1).bytes);
    EXPECT_EQ(0U, segments.at(1).entries.size());

    EXPECT_EQ("open-3",
              segments.at(2).filename);

    EXPECT_EQ("open-500",
              segments.at(3).filename);

    EXPECT_EQ(500U, log->preparedSegments.filenameCounter);
}

// readMetadata successfully checked in plenty of other tests

TEST_F(StorageSegmentedLogTest, readMetadata_missing)
{
    SegmentedLogMetadata::Metadata metadata;
    FS::removeFile(log->dir, "metadata1");
    EXPECT_FALSE(log->readMetadata("metadata1", metadata, true));
}

TEST_F(StorageSegmentedLogTest, readMetadata_corrupt)
{
    SegmentedLogMetadata::Metadata metadata;
    FS::File file = FS::openFile(log->dir, "metadata1", O_WRONLY);
    // overwrite first byte, causing checksum failure
    EXPECT_EQ(1U, FS::write(file.fd, "x", 1));
    EXPECT_FALSE(log->readMetadata("metadata1", metadata, true));
}

TEST_F(StorageSegmentedLogTest, readMetadata_unknownFormatVersion)
{
    log->metadata.set_format_version(2);
    FS::File file = FS::openFile(log->dir, "metadata1", O_WRONLY|O_TRUNC);
    Core::Buffer record = log->serializeProto(log->metadata);
    EXPECT_LT(0U, FS::write(file.fd,
                            record.getData(),
                            record.getLength()));
    SegmentedLogMetadata::Metadata metadata;
    EXPECT_DEATH(log->readMetadata("metadata1", metadata, true),
                 "The format version found in metadata1 is 2 but this code "
                 "only understands version 1");
}

TEST_F(StorageSegmentedLogTest, loadClosedSegment_missingVersion)
{
    FS::File file = FS::openFile(log->dir,
                                 closedSegment.filename,
                                 O_CREAT|O_WRONLY);
    EXPECT_DEATH(log->loadClosedSegment(closedSegment, 5000),
                 "completely empty");
}

TEST_F(StorageSegmentedLogTest, loadClosedSegment_unknownVersion)
{
    FS::File file = FS::openFile(log->dir,
                                 closedSegment.filename,
                                 O_CREAT|O_WRONLY);
    writeSegmentHeader(file, /*version=*/2);
    EXPECT_DEATH(log->loadClosedSegment(closedSegment, 5000),
                 "version.*was 2, but this code can only read version 1");
}

TEST_F(StorageSegmentedLogTest, loadClosedSegment_removeUnneeded)
{
    FS::File file = FS::openFile(log->dir,
                                 closedSegment.filename,
                                 O_CREAT|O_WRONLY);
    writeSegmentHeader(file);
    EXPECT_FALSE(log->loadClosedSegment(closedSegment, 5000));
    EXPECT_EQ(-1, FS::tryOpenFile(log->dir, closedSegment.filename,
                                  O_RDONLY).fd);
}

TEST_F(StorageSegmentedLogTest, loadClosedSegment_missingEntries)
{
    FS::File file = FS::openFile(log->dir,
                                 closedSegment.filename,
                                 O_CREAT|O_WRONLY);
    writeSegmentHeader(file);
    EXPECT_DEATH(log->loadClosedSegment(closedSegment, 1),
                 "File too short");
}

TEST_F(StorageSegmentedLogTest, loadClosedSegment_corrupt)
{
    FS::File file = FS::openFile(log->dir,
                                 closedSegment.filename,
                                 O_CREAT|O_WRONLY);
    writeSegmentHeader(file);
    FS::write(file.fd, "CRC32: haha, just kidding", 27);
    EXPECT_DEATH(log->loadClosedSegment(closedSegment, 1),
                 "corrupt");
}

TEST_F(StorageSegmentedLogTest, loadClosedSegment_extraBytes)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->closeSegment();
    log->openNewSegment();
    std::string oldName = "00000000000000000003-00000000000000000004";
    FS::File file = FS::openFile(log->dir, oldName, O_RDWR);
    uint64_t oldSize = FS::getSize(file);
    closedSegment.filename = "00000000000000000003-00000000000000000003";
    closedSegment.startIndex = 3;
    closedSegment.endIndex = 3;
    FS::rename(log->dir, oldName,
               log->dir, closedSegment.filename);
    FS::truncate(file, oldSize - 1);
    LogCabin::Core::Debug::setLogPolicy({ // expect warnings
        {"Storage/SegmentedLog", "ERROR"}
    });
    EXPECT_TRUE(log->loadClosedSegment(closedSegment, 1));
    LogCabin::Core::Debug::setLogPolicy({
        {"", "WARNING"}
    });
    EXPECT_EQ(oldSize - (oldSize - sizeof(SegmentedLog::SegmentHeader)) / 2,
              FS::getSize(file));
    construct(); // additional sanity checks
}

TEST_F(StorageSegmentedLogTest, loadClosedSegment_ok)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->closeSegment();
    log->openNewSegment();
    closedSegment.filename = "00000000000000000003-00000000000000000004";
    closedSegment.startIndex = 3;
    closedSegment.endIndex = 4;
    EXPECT_TRUE(log->loadClosedSegment(closedSegment, 1));
    FS::openFile(log->dir, closedSegment.filename, O_RDONLY); // file exists
    EXPECT_EQ(2U, closedSegment.entries.size());
}


TEST_F(StorageSegmentedLogTest, loadOpenSegment_empty)
{
    FS::File file = FS::openFile(log->dir,
                                 openSegment.filename,
                                 O_CREAT|O_WRONLY);
    LogCabin::Core::Debug::setLogPolicy({ // expect warnings
        {"Storage/SegmentedLog", "ERROR"}
    });
    EXPECT_FALSE(log->loadOpenSegment(openSegment, 1));
    LogCabin::Core::Debug::setLogPolicy({
        {"", "WARNING"}
    });
    EXPECT_EQ(-1, FS::tryOpenFile(log->dir, openSegment.filename,
                                  O_RDONLY).fd);

    file = FS::openFile(log->dir,
                        openSegment.filename,
                        O_CREAT|O_WRONLY);
    writeSegmentHeader(file);
    // no warning this time
    EXPECT_FALSE(log->loadOpenSegment(openSegment, 1));
    EXPECT_EQ(-1, FS::tryOpenFile(log->dir, openSegment.filename,
                                  O_RDONLY).fd);
}

TEST_F(StorageSegmentedLogTest, loadOpenSegment_unknownVersion)
{
    FS::File file = FS::openFile(log->dir,
                                 openSegment.filename,
                                 O_CREAT|O_WRONLY);
    writeSegmentHeader(file, /*version=*/2);
    EXPECT_DEATH(log->loadOpenSegment(openSegment, 1),
                 "version.*was 2, but this code can only read version 1");
}

TEST_F(StorageSegmentedLogTest, loadOpenSegment_removeUnneeded)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    construct();
    std::string oldName = "00000000000000000003-00000000000000000004";
    FS::rename(log->dir, oldName,
               log->dir, openSegment.filename);
    EXPECT_FALSE(log->loadOpenSegment(openSegment, 5));
    EXPECT_EQ(-1, FS::tryOpenFile(log->dir, openSegment.filename,
                                  O_RDONLY).fd);
}

TEST_F(StorageSegmentedLogTest, loadOpenSegment_corruptDelete)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    construct();
    std::string oldName = "00000000000000000003-00000000000000000004";
    FS::rename(log->dir, oldName,
               log->dir, openSegment.filename);
    FS::File file = FS::openFile(log->dir, openSegment.filename, O_RDWR);
    EXPECT_LE(0, lseek(file.fd, sizeof(SegmentedLog::SegmentHeader), SEEK_SET))
        << strerror(errno);
    EXPECT_LE(0, FS::write(file.fd, "x", 1))
        << strerror(errno);
    LogCabin::Core::Debug::setLogPolicy({ // expect warnings
        {"Storage/SegmentedLog", "ERROR"}
    });
    EXPECT_FALSE(log->loadOpenSegment(openSegment, 3));
    LogCabin::Core::Debug::setLogPolicy({
        {"", "WARNING"}
    });
    EXPECT_EQ(-1, FS::tryOpenFile(log->dir, openSegment.filename,
                                  O_RDONLY).fd);
}

TEST_F(StorageSegmentedLogTest, loadOpenSegment_truncateZeroAndOk)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    {
        FS::File oldFile =
            FS::openFile(log->dir,
                         log->getOpenSegment().filename,
                         O_RDONLY);
        FS::FileContents contents(oldFile);
        FS::File newFile =
            FS::openFile(log->dir,
                         openSegment.filename,
                         O_CREAT|O_RDWR);
        EXPECT_LT(0, FS::write(newFile.fd,
                               contents.get(0, contents.getFileLength()),
                               contents.getFileLength()));
    }
    LogCabin::Core::Debug::setLogPolicy({ // expect warnings
        {"Storage/SegmentedLog", "ERROR"}
    });
    EXPECT_TRUE(log->loadOpenSegment(openSegment, 3));
    LogCabin::Core::Debug::setLogPolicy({
        {"", "WARNING"}
    });
    EXPECT_FALSE(openSegment.isOpen);
    EXPECT_EQ(2U, openSegment.entries.size());
    EXPECT_EQ(openSegment.makeClosedFilename(), openSegment.filename);
    EXPECT_EQ(3U, openSegment.startIndex);
    EXPECT_EQ(4U, openSegment.endIndex);
}

TEST_F(StorageSegmentedLogTest, closeSegment_empty)
{
    std::string filename = log->getOpenSegment().filename;
    log->closeSegment();
    EXPECT_EQ(-1, FS::tryOpenFile(log->dir, filename, O_RDONLY).fd);
    EXPECT_EQ(0U, log->segmentsByStartIndex.size());
    log->openNewSegment();
}

TEST_F(StorageSegmentedLogTest, closeSegment_nonEmpty)
{
    log->truncatePrefix(3);
    log->append({&sampleEntry, &sampleEntry}); // index 3-4
    sync();
    log->closeSegment();
    // renamed
    FS::File file = FS::openFile(log->dir,
                                 "00000000000000000003-00000000000000000004",
                                 O_RDONLY);
    EXPECT_GT(1024U, FS::getSize(file)); // truncated to fit
    EXPECT_EQ(1U, log->segmentsByStartIndex.size());
    EXPECT_EQ("00000000000000000003-00000000000000000004",
              log->segmentsByStartIndex.at(3).filename);
    EXPECT_FALSE(log->segmentsByStartIndex.at(3).isOpen);

    log->openNewSegment(); // maintain invariants
}

// openNewSegment tested sufficiently elsewhere

TEST_F(StorageSegmentedLogTest, readProtoFromFile_binary)
{
    FS::removeFile(log->dir, "metadata1");
    FS::removeFile(log->dir, "metadata2");
    log.reset();
    log.reset(new SegmentedLog(layout.logDir,
                               SegmentedLog::Encoding::BINARY,
                               config));
    readProtoFromFileHelper();
}

TEST_F(StorageSegmentedLogTest, readProtoFromFile_text)
{
    FS::removeFile(log->dir, "metadata1");
    FS::removeFile(log->dir, "metadata2");
    log.reset();
    log.reset(new SegmentedLog(layout.logDir,
                               SegmentedLog::Encoding::TEXT,
                               config));
    readProtoFromFileHelper();
}

// serialize proto tested sufficiently by readProtoFromFile

TEST_F(StorageSegmentedLogTest, prepareNewSegment)
{
    auto ret = log->prepareNewSegment(50);
    EXPECT_EQ("open-50", ret.first);
    EXPECT_EQ(log->MAX_SEGMENT_SIZE,
              FS::getSize(ret.second));
    FS::FileContents contents(ret.second);
    EXPECT_EQ(1U, *contents.get<uint8_t>(0, 1)); // header
    for (uint64_t i = 1; i < contents.getFileLength(); ++i)
        EXPECT_EQ(0U, *contents.get<uint8_t>(i, 1));
}

// segmentPreparerMain tested plenty elsewhere, low benefit

class StorageSegmentedLogPreparedSegmentsTest : public ::testing::Test {
    typedef SegmentedLog::PreparedSegments PreparedSegments;
    typedef PreparedSegments::OpenSegment OpenSegment;
    StorageSegmentedLogPreparedSegmentsTest()
        : preparedSegments()
    {
        preparedSegments.reset(new PreparedSegments(3));
    }
    ~StorageSegmentedLogPreparedSegmentsTest()
    {
        preparedSegments.reset();
    }
    std::unique_ptr<PreparedSegments> preparedSegments;
};

void
exitCallback(SegmentedLog::PreparedSegments& preparedSegments)
{
    preparedSegments.exit();
}

TEST_F(StorageSegmentedLogPreparedSegmentsTest, foundFile)
{
    preparedSegments->foundFile(7);
    preparedSegments->foundFile(5);
    EXPECT_EQ(8U, preparedSegments->waitForDemand());
}

TEST_F(StorageSegmentedLogPreparedSegmentsTest, releaseAll)
{
    preparedSegments->submitOpenSegment({"foo", FilesystemUtil::File()});
    preparedSegments->submitOpenSegment({"bar", FilesystemUtil::File()});
    std::deque<OpenSegment> segments = preparedSegments->releaseAll();
    EXPECT_EQ(2U, segments.size());
    EXPECT_EQ("foo", segments.at(0).first);
    EXPECT_EQ("bar", segments.at(1).first);
}

TEST_F(StorageSegmentedLogPreparedSegmentsTest, submitOpenSegment)
{
    preparedSegments->submitOpenSegment({"foo", FilesystemUtil::File()});
    EXPECT_EQ(1U, preparedSegments->openSegments.size());
    EXPECT_EQ(1U, preparedSegments->produced.notificationCount);
}

TEST_F(StorageSegmentedLogPreparedSegmentsTest, waitForDemand)
{
    preparedSegments->consumed.callback =
        std::bind(exitCallback, std::ref(*preparedSegments));
    EXPECT_EQ(1U, preparedSegments->waitForDemand());
    EXPECT_EQ(2U, preparedSegments->waitForDemand());
    EXPECT_EQ(3U, preparedSegments->waitForDemand());
    EXPECT_THROW(preparedSegments->waitForDemand(),
                 Core::Util::ThreadInterruptedException);
}

TEST_F(StorageSegmentedLogPreparedSegmentsTest, waitForOpenSegment_exit)
{
    preparedSegments->quietForUnitTests = true;
    preparedSegments->produced.callback =
        std::bind(exitCallback, std::ref(*preparedSegments));
    EXPECT_THROW(preparedSegments->waitForOpenSegment(),
                 Core::Util::ThreadInterruptedException);
}

void
produceOne(SegmentedLog::PreparedSegments& preparedSegments)
{
    preparedSegments.waitForDemand();
    preparedSegments.submitOpenSegment({"foo", FilesystemUtil::File()});
}

TEST_F(StorageSegmentedLogPreparedSegmentsTest, waitForOpenSegment)
{
    preparedSegments.reset(new PreparedSegments(1));
    preparedSegments->quietForUnitTests = true;
    preparedSegments->produced.callback =
        std::bind(produceOne, std::ref(*preparedSegments));
    EXPECT_EQ("foo", preparedSegments->waitForOpenSegment().first);
    preparedSegments->waitForDemand(); // returns now
    EXPECT_EQ(1U, preparedSegments->consumed.notificationCount);
}

} // namespace LogCabin::Storage::<anonymous>
} // namespace LogCabin::Storage
} // namespace LogCabin
