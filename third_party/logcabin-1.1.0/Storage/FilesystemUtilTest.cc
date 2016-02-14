/* Copyright (c) 2011-2012 Stanford University
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
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "Core/Debug.h"
#include "Core/STLUtil.h"
#include "Storage/FilesystemUtil.h"

namespace LogCabin {
namespace Storage {

using std::string;
using std::vector;
using FilesystemUtil::File;

namespace FilesystemUtil {
namespace System {
  extern ssize_t (*writev)(int fildes,
                           const struct iovec* iov,
                           int iovcnt);
} // namespace LogCabin::Storage::FilesystemUtil::System
} // namespace LogCabin::Storage::FilesystemUtil

namespace MockWritev {

struct State {
    State() : allowWrites(), written() {}
    /**
     * Number of bytes to process in each writev call.
     * Negative values are used as errno.
     */
    std::queue<int> allowWrites;
    string written;
};
std::unique_ptr<State> state;

ssize_t
writev(int fildes, const struct iovec* iov, int iovcnt)
{
    if (state->allowWrites.empty()) {
        errno = EINVAL;
        return -1;
    }
    int allowWrite = state->allowWrites.front();
    state->allowWrites.pop();
    if (allowWrite < 0) {
        errno = -allowWrite;
        return -1;
    }

    std::string flattened;
    for (int i = 0; i < iovcnt; ++i) {
        flattened += string(static_cast<const char*>(iov[i].iov_base),
                                                     iov[i].iov_len);
    }
    state->written.append(flattened, 0, size_t(allowWrite));
    return allowWrite;
}

} // namespace LogCabin::Storage::MockWritev

class StorageFilesystemUtilTest : public ::testing::Test {
  public:
    StorageFilesystemUtilTest()
        : tmpdir(makeTmpDir())
    {
        MockWritev::state.reset(new MockWritev::State);
    }
    ~StorageFilesystemUtilTest() {
        // It's a bit dubious to be using the functions we're testing to set up
        // the test fixture. Hopefully this won't trash your home directory.
        FilesystemUtil::remove(tmpdir.path);
        FilesystemUtil::System::writev = ::writev;
        MockWritev::state.reset();
    }
    File makeTmpDir() {
        std::string path = FilesystemUtil::mkdtemp();
        return File(open(path.c_str(), O_RDONLY|O_DIRECTORY), path);
    }
    File tmpdir;
};

namespace FS = FilesystemUtil;

TEST_F(StorageFilesystemUtilTest, allocate) {
    FS::File file(FS::openFile(tmpdir, "a", O_RDWR|O_CREAT));
    FS::write(file.fd, "hello world", 11); // no null byte
    char buf[15];

    FS::allocate(file, 5, 10);
    EXPECT_EQ(15U, FS::getSize(file));
    EXPECT_EQ(15, pread(file.fd, buf, sizeof(buf), 0));
    EXPECT_STREQ("hello world", buf);

    FS::allocate(file, 0, 5);
    EXPECT_EQ(15, pread(file.fd, buf, sizeof(buf), 0));
    EXPECT_STREQ("hello world", buf);

    EXPECT_DEATH(FS::allocate(File(), 0, 10),
                 "Could not posix_fallocate");
}

TEST_F(StorageFilesystemUtilTest, dup) {
    File d1 = FS::dup(tmpdir);
    EXPECT_NE(d1.fd, tmpdir.fd);
    EXPECT_GE(d1.fd, 0);
    EXPECT_EQ(d1.path, tmpdir.path);
    EXPECT_DEATH(FS::dup(File()),
                 "Dup failed");
}

TEST_F(StorageFilesystemUtilTest, fsync) {
    FilesystemUtil::skipFsync = false;
    FS::fsync(tmpdir);
    EXPECT_DEATH(FS::fsync(File()),
                 "Could not fsync");
}

TEST_F(StorageFilesystemUtilTest, fdatasync) {
    FilesystemUtil::skipFsync = false;
    FS::fdatasync(tmpdir);
    EXPECT_DEATH(FS::fdatasync(File()),
                 "Could not fdatasync");
}

TEST_F(StorageFilesystemUtilTest, flock) {
    FS::File tmpdir2(FS::openDir(tmpdir.path));
    FS::flock(tmpdir, LOCK_EX|LOCK_NB);
    EXPECT_DEATH(FS::flock(tmpdir2, LOCK_EX|LOCK_NB),
                 "Could not flock.*temporarily");
    EXPECT_DEATH(FS::flock(FilesystemUtil::File(), LOCK_EX),
                 "Could not flock.*Bad file");
}

TEST_F(StorageFilesystemUtilTest, tryFlock) {
    FS::File tmpdir2(FS::openDir(tmpdir.path));
    EXPECT_EQ("", FS::tryFlock(tmpdir, LOCK_EX|LOCK_NB));
    std::string e = FS::tryFlock(tmpdir2, LOCK_EX|LOCK_NB);
    EXPECT_NE(e.npos, e.find("temporarily")) << e;
    EXPECT_DEATH(FS::tryFlock(FilesystemUtil::File(), LOCK_EX),
                 "Could not flock.*Bad file");
}

TEST_F(StorageFilesystemUtilTest, getSize) {
    FS::File file(FS::openFile(tmpdir, "a", O_RDWR|O_CREAT));
    EXPECT_EQ(0U, FS::getSize(file));
    if (FS::write(file.fd, "hello world!", 13) != 13)
        PANIC("write failed");
    EXPECT_EQ(13U, FS::getSize(file));
}

TEST_F(StorageFilesystemUtilTest, ls) {
    EXPECT_DEATH(FilesystemUtil::ls("/path/does/not/exist"),
                 "Could not list contents");
    // TODO(ongaro): Test readdir_r failure.

    EXPECT_EQ((vector<string> {}),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir.path)));

    EXPECT_EQ(0, mkdir((tmpdir.path + "/a").c_str(), 0755));
    int fd = open((tmpdir.path + "/b").c_str(), O_WRONLY|O_CREAT, 0644);
    EXPECT_LE(0, fd);
    EXPECT_EQ(0, close(fd));
    EXPECT_EQ(0, mkdir((tmpdir.path + "/c").c_str(), 0755));
    EXPECT_EQ((vector<string> { "a", "b", "c" }),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir.path)));
}

TEST_F(StorageFilesystemUtilTest, ls_fd) {
    EXPECT_DEATH(FilesystemUtil::ls(File(-1, "/path/does/not/exist")),
                 "Bad file descriptor");
    EXPECT_EQ((vector<string> {}),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir)));

    EXPECT_EQ(0, mkdir((tmpdir.path + "/a").c_str(), 0755));
    int fd = open((tmpdir.path + "/b").c_str(), O_WRONLY|O_CREAT, 0644);
    EXPECT_LE(0, fd);
    EXPECT_EQ(0, close(fd));
    EXPECT_EQ(0, mkdir((tmpdir.path + "/c").c_str(), 0755));
    EXPECT_EQ((vector<string> { "a", "b", "c" }),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir)));
}

TEST_F(StorageFilesystemUtilTest, openDir) {
    EXPECT_DEATH(FS::openDir(tmpdir.path + "/a/b"),
                 "Could not create directory");
    File d1 = FS::openDir(tmpdir.path + "/a");
    EXPECT_EQ(tmpdir.path + "/a", d1.path);
    EXPECT_LE(0, d1.fd);
    File d2 = FS::openDir(tmpdir.path + "/a");
    EXPECT_EQ(tmpdir.path + "/a", d2.path);
    EXPECT_LE(0, d2.fd);
}

TEST_F(StorageFilesystemUtilTest, openDir_fd) {
    EXPECT_DEATH(FS::openDir(tmpdir, "a/b"),
                 "Could not create directory");
    File d1 = FS::openDir(tmpdir, "a");
    EXPECT_EQ(tmpdir.path + "/a", d1.path);
    EXPECT_LE(0, d1.fd);
    File d2 = FS::openDir(tmpdir, "a");
    EXPECT_EQ(tmpdir.path + "/a", d2.path);
    EXPECT_LE(0, d2.fd);
}

TEST_F(StorageFilesystemUtilTest, openFile) {
    EXPECT_DEATH(openFile(tmpdir, "d", O_RDONLY),
                 "Could not open");
    File f = openFile(tmpdir, "d", O_RDONLY|O_CREAT);
    EXPECT_EQ(tmpdir.path + "/d", f.path);
    EXPECT_LE(0, f.fd);
}

TEST_F(StorageFilesystemUtilTest, tryOpenFile) {
    FS::openDir(tmpdir.path + "/d");
    EXPECT_DEATH(openFile(tmpdir, "d", O_WRONLY),
                 "Could not open");
    File f1 = tryOpenFile(tmpdir, "e", O_RDONLY|O_CREAT);
    EXPECT_EQ(tmpdir.path + "/e", f1.path);
    EXPECT_LE(0, f1.fd);
    File f2 = tryOpenFile(tmpdir, "f", O_RDONLY);
    EXPECT_EQ("", f2.path);
    EXPECT_EQ(-1, f2.fd);
}

// This test makes sure we call rewinddir after fdopendir. This is needed at
// least on eglibc v2.13.
TEST_F(StorageFilesystemUtilTest, ls_rewindDir) {
    EXPECT_EQ(0, mkdir((tmpdir.path + "/a").c_str(), 0755));
    EXPECT_EQ((vector<string> { "a" }),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir)));
    // If this second ls comes out blank, it's probably because rewinddir was
    // not called.
    EXPECT_EQ((vector<string> { "a" }),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir)));
}

TEST_F(StorageFilesystemUtilTest, remove) {
    // does not exist
    FilesystemUtil::remove(tmpdir.path + "/a");

    // dir exists with no children
    EXPECT_EQ(0, mkdir((tmpdir.path + "/b").c_str(), 0755));
    FilesystemUtil::remove(tmpdir.path + "/b");

    // file exists with no children
    int fd = open((tmpdir.path + "/c").c_str(), O_WRONLY|O_CREAT, 0644);
    EXPECT_LE(0, fd);
    EXPECT_EQ(0, close(fd));
    FilesystemUtil::remove(tmpdir.path + "/c");

    // dir exists with children
    EXPECT_EQ(0, mkdir((tmpdir.path + "/d").c_str(), 0755));
    EXPECT_EQ(0, mkdir((tmpdir.path + "/d/e").c_str(), 0755));
    EXPECT_EQ(0, mkdir((tmpdir.path + "/d/f").c_str(), 0755));
    FilesystemUtil::remove(tmpdir.path + "/d");

    EXPECT_EQ((vector<string> {}),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir)));

    // error
    EXPECT_EQ(0, mkdir((tmpdir.path + "/g").c_str(), 0755));
    EXPECT_DEATH(FilesystemUtil::remove(tmpdir.path + "/g/."),
                 "Could not remove");
}

TEST_F(StorageFilesystemUtilTest, removeFile) {
    FS::removeFile(tmpdir, "a");
    FS::openFile(tmpdir, "b", O_RDONLY|O_CREAT);
    FS::removeFile(tmpdir, "b");
    EXPECT_EQ((vector<string> {}),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir)));
}

TEST_F(StorageFilesystemUtilTest, rename) {
    File ac = openDir(tmpdir, "a");
    openDir(tmpdir, "b");
    FS::rename(tmpdir, "a", tmpdir, "c");
    FS::rename(tmpdir, "b", ac, "d");
    EXPECT_EQ((vector<string> {"c"}),
              Core::STLUtil::sorted(FilesystemUtil::ls(tmpdir)));
    EXPECT_EQ((vector<string> {"d"}),
              Core::STLUtil::sorted(FilesystemUtil::ls(ac)));
}

TEST_F(StorageFilesystemUtilTest, syncDir) {
    FilesystemUtil::skipFsync = false;
    // I don't know of a way to observe that this does anything,
    // but at least we can run through it and make sure nothing panics.
    // -Diego
    FilesystemUtil::syncDir(tmpdir.path);
    FilesystemUtil::syncDir(tmpdir.path + "/..");
    EXPECT_DEATH(FilesystemUtil::syncDir(tmpdir.path + "/a"),
                 "open");
}

TEST_F(StorageFilesystemUtilTest, truncate) {
    FS::File file(FS::openFile(tmpdir, "a", O_RDWR|O_CREAT));
    FS::write(file.fd, "hello world", 11); // no null byte
    char buf[15];

    FS::truncate(file, 15);
    EXPECT_EQ(15U, FS::getSize(file));
    EXPECT_EQ(15, pread(file.fd, buf, sizeof(buf), 0));
    EXPECT_STREQ("hello world", buf);

    FS::truncate(file, 5);
    EXPECT_EQ(5, pread(file.fd, buf, sizeof(buf), 0));
    buf[5] = '\0';
    EXPECT_STREQ("hello", buf);

    EXPECT_DEATH(FS::truncate(File(), 10),
                 "Could not ftruncate");
}

TEST_F(StorageFilesystemUtilTest, mkdtemp) {
    std::string a = FilesystemUtil::mkdtemp();
    std::string b = FilesystemUtil::mkdtemp();
    EXPECT_NE(a, b);
    Storage::FilesystemUtil::remove(a);
    Storage::FilesystemUtil::remove(b);
}

TEST_F(StorageFilesystemUtilTest, writeCommon) {
    int fd = open((tmpdir.path + "/a").c_str(), O_RDWR|O_CREAT, 0644);
    EXPECT_LE(0, fd);
    EXPECT_EQ(13, FilesystemUtil::write(fd, {
            {"hello ", 6},
            {"", 0},
            {"world!", 7},
        }));
    char buf[13];
    EXPECT_EQ(13, pread(fd, buf, sizeof(buf), 0));
    EXPECT_STREQ("hello world!", buf);
    EXPECT_EQ(0, close(fd));

}

TEST_F(StorageFilesystemUtilTest, writeInterruption) {
    MockWritev::state->allowWrites.push(-EINTR);
    MockWritev::state->allowWrites.push(0);
    MockWritev::state->allowWrites.push(1);
    MockWritev::state->allowWrites.push(8);
    MockWritev::state->allowWrites.push(4);
    FilesystemUtil::System::writev = MockWritev::writev;
    EXPECT_EQ(13, FilesystemUtil::write(100, {
            {"hello ", 6},
            {"", 0},
            {"world!", 7},
        }));
    EXPECT_EQ(13U, MockWritev::state->written.size());
    EXPECT_STREQ("hello world!", MockWritev::state->written.c_str());
}

class StorageFileContentsTest : public StorageFilesystemUtilTest {
    StorageFileContentsTest()
        : rawFile(FilesystemUtil::openFile(tmpdir, "a", O_RDWR|O_CREAT))
    {
        if (FilesystemUtil::write(rawFile.fd, "hello world!", 13) != 13)
            PANIC("write failed");
    }
    File rawFile;
};

TEST_F(StorageFileContentsTest, constructor) {
    // extra parenthesis to disambiguate parsing, see
    // http://en.wikipedia.org/wiki/Most_vexing_parse
    EXPECT_DEATH(FilesystemUtil::FileContents x((File())),
                 "Bad file descriptor");
}

TEST_F(StorageFileContentsTest, getFileLength) {
    FilesystemUtil::FileContents file(rawFile);
    EXPECT_EQ(13U, file.getFileLength());
}

TEST_F(StorageFileContentsTest, copy) {
    FilesystemUtil::FileContents file(rawFile);
    char buf[13];
    strcpy(buf, "cccccccccccc"); // NOLINT
    file.copy(0, buf, 13);
    EXPECT_STREQ("hello world!", buf);
    strcpy(buf, "cccccccccccc"); // NOLINT
    file.copy(13, buf, 0); // should be ok
    file.copy(15, buf, 0); // should be ok
    EXPECT_STREQ("cccccccccccc", buf);
    EXPECT_DEATH(file.copy(0, buf, 14),
                 "ERROR");
    EXPECT_DEATH(file.copy(1, buf, 13),
                 "ERROR");
}

TEST_F(StorageFileContentsTest, copyPartial) {
    FilesystemUtil::FileContents file(rawFile);
    char buf[13];
    strcpy(buf, "cccccccccccc"); // NOLINT
    EXPECT_EQ(13U, file.copyPartial(0, buf, 13));
    EXPECT_STREQ("hello world!", buf);
    strcpy(buf, "cccccccccccc"); // NOLINT
    EXPECT_EQ(0U, file.copyPartial(13, buf, 0));
    EXPECT_EQ(0U, file.copyPartial(15, buf, 0));
    EXPECT_STREQ("cccccccccccc", buf);
    EXPECT_EQ(13U, file.copyPartial(0, buf, 14));
    EXPECT_STREQ("hello world!", buf);
    strcpy(buf, "cccccccccccc"); // NOLINT
    EXPECT_EQ(12U, file.copyPartial(1, buf, 13));
    EXPECT_STREQ("ello world!", buf);
}

TEST_F(StorageFileContentsTest, get) {
    FilesystemUtil::FileContents file(rawFile);
    EXPECT_STREQ("hello world!",
                 file.get<char>(0, 13));
    file.get<char>(13, 0); // should be ok, result doesn't matter
    file.get<char>(15, 0); // should be ok, result doesn't matter
    EXPECT_DEATH(file.get(0, 14),
                 "ERROR");
    EXPECT_DEATH(file.get(1, 13),
                 "ERROR");
}

TEST_F(StorageFileContentsTest, emptyFile) {
    File emptyFile = FS::openFile(tmpdir, "empty", O_CREAT|O_RDONLY);
    FS::FileContents file(emptyFile);
    EXPECT_EQ(0U, file.getFileLength());
    file.copy(0, NULL, 0);
    EXPECT_EQ(0U, file.copyPartial(0, NULL, 0));
    EXPECT_EQ(0U, file.copyPartial(0, NULL, 1));
    EXPECT_EQ(0U, file.copyPartial(1, NULL, 1));
    file.get(0, 0);
    file.get(1, 0);
}

} // namespace LogCabin::Storage
} // namespace LogCabin
