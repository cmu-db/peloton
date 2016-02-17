/* Copyright (c) 2013 Stanford University
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
#include <sys/stat.h>

#include "build/Core/ProtoBufTest.pb.h"
#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Core/STLUtil.h"
#include "Storage/FilesystemUtil.h"
#include "Storage/Layout.h"
#include "Storage/SnapshotFile.h"

namespace LogCabin {
namespace Storage {
namespace SnapshotFile {
namespace {

class StorageSnapshotFileTest : public ::testing::Test {
  public:
    StorageSnapshotFileTest()
        : layout()
        , m1()
    {
        layout.initTemporary();
        m1.set_field_a(10);
        m1.set_field_b(20);
    }
    ~StorageSnapshotFileTest() {
    }
    Storage::Layout layout;
    ProtoBuf::TestMessage m1;
};


TEST_F(StorageSnapshotFileTest, basic)
{
    {
        Writer writer(layout);
        writer.writeMessage(m1);
        writer.save();
    }
    {
        Reader reader(layout);
        ProtoBuf::TestMessage out;
        EXPECT_EQ("", reader.readMessage(out));
        EXPECT_EQ(m1, out);
    }
}

TEST_F(StorageSnapshotFileTest, discardPartialSnapshots)
{
    FilesystemUtil::openFile(layout.snapshotDir, "partial.1",
                             O_WRONLY|O_CREAT);
    FilesystemUtil::openFile(layout.snapshotDir, "partial.2",
                             O_WRONLY|O_CREAT);
    FilesystemUtil::openFile(layout.snapshotDir, "partial.3",
                             O_WRONLY|O_CREAT);
    FilesystemUtil::openFile(layout.snapshotDir, "snapshot",
                             O_WRONLY|O_CREAT);
    FilesystemUtil::openFile(layout.snapshotDir, "misc",
                             O_WRONLY|O_CREAT);
    // expect warning
    Core::Debug::setLogPolicy({
        {"Storage/SnapshotFile.cc", "ERROR"}
    });
    discardPartialSnapshots(layout);
    std::vector<std::string> files =
        Core::STLUtil::sorted(FilesystemUtil::ls(layout.snapshotDir));
    EXPECT_EQ((std::vector<std::string> {
                "misc",
                "snapshot"
               }), files);
}

TEST_F(StorageSnapshotFileTest, reader_fileNotFound)
{
    EXPECT_THROW(Reader reader(layout), std::runtime_error);
}

TEST_F(StorageSnapshotFileTest, getBytesRead)
{
    {
        Writer writer(layout);
        uint32_t d = 0xdeadbeef;
        writer.writeRaw(&d, sizeof(d));
        writer.save();
    }
    {
        Reader reader(layout);
        uint32_t x = 0;
        EXPECT_EQ(0U, reader.getBytesRead());
        reader.readRaw(&x, 1);
        EXPECT_EQ(1U, reader.getBytesRead());
        reader.readRaw(&x, 4);
        EXPECT_EQ(4U, reader.getBytesRead());
        reader.readRaw(&x, 2);
        EXPECT_EQ(4U, reader.getBytesRead());
    }
}

TEST_F(StorageSnapshotFileTest, readMessage)
{
    uint64_t m1bytes = 0;
    {
        Writer writer(layout);
        writer.writeMessage(m1);
        m1bytes = writer.getBytesWritten();
        uint32_t len = 100;
        len = htobe32(len);
        writer.writeRaw(&len, sizeof(len));
        writer.writeRaw(&len, 1);
        writer.save();
    }
    {
        Reader reader(layout);
        ProtoBuf::TestMessage out;
        EXPECT_EQ("", reader.readMessage(out));
        EXPECT_EQ(m1, out);
        EXPECT_EQ(m1bytes, reader.getBytesRead());
        EXPECT_NE("", reader.readMessage(out));
        EXPECT_EQ(m1bytes + 4, reader.getBytesRead());
        uint32_t x;
        EXPECT_EQ(1U, reader.readRaw(&x, 1));
        EXPECT_NE("", reader.readMessage(out));
        EXPECT_EQ(m1bytes + 5, reader.getBytesRead());
    }
}

TEST_F(StorageSnapshotFileTest, readRaw)
{
    {
        Writer writer(layout);
        uint32_t d = 0xdeadbeef;
        writer.writeRaw(&d, sizeof(d));
        writer.save();
    }
    {
        Reader reader(layout);
        uint32_t x = 0;
        reader.readRaw(&x, sizeof(x));
        EXPECT_EQ(0xdeadbeefU, x);
    }
    {
        Reader reader(layout);
        uint32_t x = 0;
        EXPECT_EQ(1U, reader.readRaw(&x, 1));
        EXPECT_EQ(0xefU, x);
        x = 0;
        EXPECT_EQ(3U, reader.readRaw(&x, 4));
        EXPECT_EQ(0xdeadbeU, x);
    }
}

TEST_F(StorageSnapshotFileTest, writer_orphanDiscarded)
{
    // expect warning
    Core::Debug::setLogPolicy({
        {"Storage/SnapshotFile.cc", "ERROR"}
    });
    {
        Writer writer(layout);
        uint32_t d = 482;
        writer.writeRaw(&d, sizeof(d));
    }
    std::vector<std::string> files =
        Core::STLUtil::sorted(FilesystemUtil::ls(layout.snapshotDir));
    EXPECT_EQ(0U, files.size()) <<
        Core::StringUtil::join(files, " ");
}

TEST_F(StorageSnapshotFileTest, writer_discard)
{
    {
        Writer writer(layout);
        uint32_t d = 482;
        writer.writeRaw(&d, sizeof(d));
        writer.discard();
    }
    std::vector<std::string> files = FilesystemUtil::ls(layout.snapshotDir);
    EXPECT_EQ(0U, files.size()) <<
        Core::StringUtil::join(files, " ");
}

TEST_F(StorageSnapshotFileTest, writer_flushToOS_continue)
{
    {
        Writer writer(layout);
        uint32_t d = 482;
        writer.writeRaw(&d, sizeof(d));
        writer.flushToOS();
        d = 998;
        writer.writeRaw(&d, sizeof(d));
        writer.save();
    }
    {
        Reader reader(layout);
        uint32_t x = 0;
        reader.readRaw(&x, sizeof(x));
        EXPECT_EQ(482U, x);
        reader.readRaw(&x, sizeof(x));
        EXPECT_EQ(998U, x);
    }
}

// tests flushToOS and seekToEnd
TEST_F(StorageSnapshotFileTest, writer_forking)
{
    {
        Writer writer(layout);
        uint32_t d = 482;
        writer.writeRaw(&d, sizeof(d));
        writer.flushToOS();
        EXPECT_EQ(4U, writer.getBytesWritten());
        pid_t pid = fork();
        ASSERT_LE(0, pid);
        uint64_t start = *writer.sharedBytesWritten.value;
        if (pid == 0) { // child
            d = 127;
            writer.writeRaw(&d, sizeof(d));
            EXPECT_LT(start, *writer.sharedBytesWritten.value);
            writer.flushToOS();
            EXPECT_EQ(8U, writer.getBytesWritten());
            _exit(0);
        } else { // parent
            int status = 0;
            pid = waitpid(pid, &status, 0);
            EXPECT_LE(0, pid);
            EXPECT_TRUE(WIFEXITED(status));
            EXPECT_EQ(0, WEXITSTATUS(status));
            EXPECT_LT(start, *writer.sharedBytesWritten.value);
            writer.seekToEnd();
            EXPECT_EQ(8U, writer.getBytesWritten());
            d = 998;
            writer.writeRaw(&d, sizeof(d));
            writer.save();
        }
    }
    {
        Reader reader(layout);
        uint32_t x = 0;
        EXPECT_EQ(sizeof(x), reader.readRaw(&x, sizeof(x)));
        EXPECT_EQ(482U, x);
        EXPECT_EQ(sizeof(x), reader.readRaw(&x, sizeof(x)));
        EXPECT_EQ(127U, x);
        EXPECT_EQ(sizeof(x), reader.readRaw(&x, sizeof(x)));
        EXPECT_EQ(998U, x);
    }
}

TEST_F(StorageSnapshotFileTest, getBytesWritten)
{
    Writer writer(layout);
    EXPECT_EQ(0U, writer.getBytesWritten());
    uint32_t d = 0xdeadbeef;
    writer.writeRaw(&d, sizeof(d));
    EXPECT_EQ(4U, writer.getBytesWritten());
    writer.writeMessage(m1);
    EXPECT_LT(4U, writer.getBytesWritten());
    writer.discard();
}

TEST_F(StorageSnapshotFileTest, sharedBytesWritten)
{
    Writer writer(layout);
    EXPECT_EQ(0U, *writer.sharedBytesWritten.value);
    *writer.sharedBytesWritten.value = 1000;
    uint32_t d = 0xdeadbeef;
    writer.writeRaw(&d, sizeof(d));
    EXPECT_EQ(1004U, *writer.sharedBytesWritten.value);
    writer.writeMessage(m1);
    EXPECT_LT(1004U, *writer.sharedBytesWritten.value);
    writer.discard();

}


// writeMessage tested with readMessage above

// writeRaw tested with readRaw above

} // namespace LogCabin::Storage::SnapshotFile::<anonymous>
} // namespace LogCabin::Storage::SnapshotFile
} // namespace LogCabin::Storage
} // namespace LogCabin
