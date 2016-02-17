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

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Core/Util.h"
#include "Storage/FilesystemUtil.h"

namespace LogCabin {
namespace Storage {
namespace FilesystemUtil {

bool skipFsync = false;

File::File()
    : fd(-1)
    , path()
{
}

File::File(File&& other)
    : fd(other.fd)
    , path(other.path)
{
    other.fd = -1;
    other.path.clear();
}

File::File(int fd, std::string path)
    : fd(fd)
    , path(path)
{
}

File::~File()
{
    close();
}

File&
File::operator=(File&& other)
{
    std::swap(fd, other.fd);
    std::swap(path, other.path);
    return *this;
}

void
File::close()
{
    if (fd < 0)
        return;
    if (::close(fd) != 0) {
        PANIC("Failed to close file %s: %s",
              path.c_str(), strerror(errno));
    }
    fd = -1;
    path.clear();
}

int
File::release()
{
    int ret = fd;
    fd = -1;
    path.clear();
    return ret;
}

void
allocate(const File& file, uint64_t offset, uint64_t bytes)
{
    int errnum = ::posix_fallocate(file.fd,
                                   Core::Util::downCast<off_t>(offset),
                                   Core::Util::downCast<off_t>(bytes));
    if (errnum != 0) {
        PANIC("Could not posix_fallocate bytes [%lu, %lu) of %s: %s",
              offset, offset + bytes, file.path.c_str(), strerror(errnum));
    }
}

File
dup(const File& file)
{
    int newFd = ::dup(file.fd);
    if (newFd == -1) {
        PANIC("Dup failed on fd %d for path %s: %s",
              file.fd, file.path.c_str(), strerror(errno));
    }
    return File(newFd, file.path);
}

void
fsync(const File& file)
{
    if (skipFsync)
        return;
    if (::fsync(file.fd) != 0) {
        PANIC("Could not fsync %s: %s",
              file.path.c_str(), strerror(errno));
    }
}

void
fdatasync(const File& file)
{
    if (skipFsync)
        return;
    if (::fdatasync(file.fd) != 0) {
        PANIC("Could not fdatasync %s: %s",
              file.path.c_str(), strerror(errno));
    }
}


void
flock(const File& file, int operation)
{
    std::string e = tryFlock(file, operation);
    if (!e.empty())
        PANIC("%s", e.c_str());
}

std::string
tryFlock(const File& file, int operation)
{
    if (::flock(file.fd, operation) == 0)
        return std::string();
    int error = errno;
    std::string msg = Core::StringUtil::format(
        "Could not flock('%s', %s): %s",
        file.path.c_str(),
        Core::StringUtil::flags(operation,
                                {{LOCK_SH, "LOCK_SH"},
                                 {LOCK_EX, "LOCK_EX"},
                                 {LOCK_UN, "LOCK_UN"},
                                 {LOCK_NB, "LOCK_NB"}}).c_str(),
        strerror(error));
    if (error == EWOULDBLOCK)
        return msg;
    else
        PANIC("%s", msg.c_str());
}

uint64_t
getSize(const File& file)
{
    struct stat stat;
    if (fstat(file.fd, &stat) != 0) {
        PANIC("Could not stat %s: %s",
              file.path.c_str(), strerror(errno));
    }
    return Core::Util::downCast<uint64_t>(stat.st_size);
}

std::vector<std::string>
lsHelper(DIR* dir, const std::string& path)
{
    if (dir == NULL) {
        PANIC("Could not list contents of %s: %s",
              path.c_str(), strerror(errno));
    }

    // If dir was opened with fdopendir and was read from previously, this is
    // needed to rewind the directory, at least on eglibc v2.13. The unit test
    // "ls_RewindDir" shows the exact problem.
    rewinddir(dir);

    std::vector<std::string> contents;
    while (true) {
        struct dirent entry;
        struct dirent* entryp;
        if (readdir_r(dir, &entry, &entryp) != 0) {
            PANIC("readdir(%s) failed: %s",
                  path.c_str(), strerror(errno));
        }
        if (entryp == NULL) // no more entries
            break;
        const std::string name = entry.d_name;
        if (name == "." || name == "..")
            continue;
        contents.push_back(name);
    }

    if (closedir(dir) != 0) {
        WARNING("closedir(%s) failed: %s",
                path.c_str(), strerror(errno));
    }

    return contents;
}

std::vector<std::string>
ls(const std::string& path)
{
    return lsHelper(opendir(path.c_str()), path);
}

std::vector<std::string>
ls(const File& dir)
{
    return lsHelper(fdopendir(dup(dir).release()), dir.path);
}

File
openDir(const std::string& path)
{
    assert(!path.empty());
    int r = mkdir(path.c_str(), 0755);
    if (r == 0) {
        FilesystemUtil::syncDir(path + "/..");
    } else {
        if (errno != EEXIST) {
            PANIC("Could not create directory %s: %s",
                  path.c_str(), strerror(errno));
        }
    }
    // It'd be awesome if one could do O_RDONLY|O_CREAT|O_DIRECTORY here,
    // but at least on eglibc v2.13, this combination of flags creates a
    // regular file!
    int fd = open(path.c_str(), O_RDONLY|O_DIRECTORY);
    if (fd == -1) {
        PANIC("Could not open %s: %s",
              path.c_str(), strerror(errno));
    }
    return File(fd, path);
}

File
openDir(const File& dir, const std::string& child)
{
    assert(!Core::StringUtil::startsWith(child, "/"));
    int r = mkdirat(dir.fd, child.c_str(), 0755);
    if (r == 0) {
        fsync(dir);
    } else {
        if (errno != EEXIST) {
            PANIC("Could not create directory %s/%s: %s",
                  dir.path.c_str(), child.c_str(), strerror(errno));
        }
    }
    // It'd be awesome if one could do O_RDONLY|O_CREAT|O_DIRECTORY here,
    // but at least on eglibc v2.13, this combination of flags creates a
    // regular file!
    int fd = openat(dir.fd, child.c_str(), O_RDONLY|O_DIRECTORY);
    if (fd == -1) {
        PANIC("Could not open %s/%s: %s",
              dir.path.c_str(), child.c_str(), strerror(errno));
    }
    return File(fd, dir.path + "/" + child);
}

File
openFile(const File& dir, const std::string& child, int flags)
{
    assert(!Core::StringUtil::startsWith(child, "/"));
    int fd = openat(dir.fd, child.c_str(), flags, 0644);
    if (fd == -1) {
        PANIC("Could not open %s/%s: %s",
              dir.path.c_str(), child.c_str(), strerror(errno));
    }
    return File(fd, dir.path + "/" + child);
}

File
tryOpenFile(const File& dir, const std::string& child, int flags)
{
    assert(!Core::StringUtil::startsWith(child, "/"));
    int fd = openat(dir.fd, child.c_str(), flags, 0644);
    if (fd == -1) {
        if (errno == EEXIST || errno == ENOENT)
            return File();
        PANIC("Could not open %s/%s: %s",
              dir.path.c_str(), child.c_str(), strerror(errno));
    }
    return File(fd, dir.path + "/" + child);
}

void
remove(const std::string& path)
{
    while (true) {
        if (::remove(path.c_str()) == 0)
            return;
        if (errno == ENOENT) {
            return;
        } else if (errno == EEXIST || errno == ENOTEMPTY) {
            std::vector<std::string> children = ls(path);
            for (auto it = children.begin(); it != children.end(); ++it)
                remove(path + "/" + *it);
            continue;
        } else {
            PANIC("Could not remove %s: %s", path.c_str(), strerror(errno));
        }
    }
}

void
removeFile(const File& dir, const std::string& path)
{
    assert(!Core::StringUtil::startsWith(path, "/"));
    if (::unlinkat(dir.fd, path.c_str(), 0) == 0)
        return;
    if (errno == ENOENT)
        return;
    PANIC("Could not remove %s/%s: %s",
          dir.path.c_str(), path.c_str(), strerror(errno));
}

void
rename(const File& oldDir, const std::string& oldChild,
       const File& newDir, const std::string& newChild)
{
    assert(!Core::StringUtil::startsWith(oldChild, "/"));
    assert(!Core::StringUtil::startsWith(newChild, "/"));
    if (::renameat(oldDir.fd, oldChild.c_str(),
                   newDir.fd, newChild.c_str()) == 0)
        return;
    PANIC("Could not rename %s/%s to %s/%s: %s",
          oldDir.path.c_str(), oldChild.c_str(),
          newDir.path.c_str(), newChild.c_str(),
          strerror(errno));
}

void
syncDir(const std::string& path)
{
    if (skipFsync)
        return;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        PANIC("Could not open %s: %s",
              path.c_str(), strerror(errno));
    }
    if (::fsync(fd) != 0) {
        PANIC("Could not fsync %s: %s",
              path.c_str(), strerror(errno));
    }
    if (close(fd) != 0) {
        WARNING("Failed to close file %s: %s",
                path.c_str(), strerror(errno));
    }
}

void
truncate(const File& file, uint64_t bytes)
{
    if (::ftruncate(file.fd, Core::Util::downCast<off_t>(bytes)) != 0) {
        PANIC("Could not ftruncate %s: %s",
              file.path.c_str(),
              strerror(errno));
    }
}

std::string
mkdtemp()
{
    char d[] = "/tmp/logcabinXXXXXX";
    const char* path = ::mkdtemp(d);
    if (path == NULL)
        PANIC("Couldn't create temporary directory");
    return path;
}

namespace System {
// This is mocked out in some unit tests.
ssize_t (*writev)(int fildes,
                  const struct iovec* iov,
                  int iovcnt) = ::writev;
}

ssize_t
write(int fildes, const void* data, uint64_t dataLen)
{
    return write(fildes, {{data, dataLen}});
}

ssize_t
write(int fildes,
       std::initializer_list<std::pair<const void*, uint64_t>> data)
{
    using Core::Util::downCast;
    size_t totalBytes = 0;
    uint64_t iovcnt = data.size();
    struct iovec iov[iovcnt];
    uint64_t i = 0;
    for (auto it = data.begin(); it != data.end(); ++it) {
        iov[i].iov_base = const_cast<void*>(it->first);
        iov[i].iov_len = it->second;
        totalBytes += it->second;
        ++i;
    }

    size_t bytesRemaining = totalBytes;
    while (true) {
         ssize_t written = System::writev(fildes, iov, downCast<int>(iovcnt));
         if (written == -1) {
             if (errno == EINTR)
                 continue;
             return -1;
         }
         bytesRemaining = downCast<size_t>(bytesRemaining -
                                           downCast<size_t>(written));
         if (bytesRemaining == 0)
             return downCast<ssize_t>(totalBytes);
         for (uint64_t i = 0; i < iovcnt; ++i) {
             if (iov[i].iov_len < static_cast<size_t>(written)) {
                 written -= iov[i].iov_len;
                 iov[i].iov_len = 0;
             } else if (iov[i].iov_len >= static_cast<size_t>(written)) {
                 iov[i].iov_len = iov[i].iov_len - downCast<size_t>(written);
                 iov[i].iov_base = (static_cast<char*>(iov[i].iov_base) +
                                    written);
                 break;
             }
         }
    }
}

// class FileContents

FileContents::FileContents(const File& origFile)
    : file(dup(origFile))
    , fileLen(getSize(file))
    , map(NULL)
{
    // len of 0 for empty files results in invalid argument
    if (fileLen > 0) {
        map = mmap(NULL, fileLen, PROT_READ, MAP_SHARED, file.fd, 0);
        if (map == MAP_FAILED) {
            PANIC("Could not map %s: %s",
                  file.path.c_str(), strerror(errno));
        }
    }
}

FileContents::~FileContents()
{
    if (map == NULL)
        return;
    if (munmap(const_cast<void*>(map), fileLen) != 0) {
        WARNING("Failed to munmap file %s: %s",
                file.path.c_str(), strerror(errno));
    }
}

void
FileContents::copy(uint64_t offset, void* buf, uint64_t length)
{
    if (copyPartial(offset, buf, length) != length) {
        PANIC("File %s too short or corrupt",
              file.path.c_str());
    }
}

uint64_t
FileContents::copyPartial(uint64_t offset, void* buf, uint64_t maxLength)
{
    if (offset >= fileLen)
        return 0;
    uint64_t length = std::min(fileLen - offset, maxLength);
    memcpy(buf, static_cast<const char*>(map) + offset, length);
    return length;
}

const void*
FileContents::getHelper(uint64_t offset, uint64_t length)
{
    if (length != 0 && offset + length > fileLen) {
        PANIC("File %s too short or corrupt",
              file.path.c_str());
    }
    return static_cast<const char*>(map) + offset;
}

} // namespace LogCabin::Storage::FilesystemUtil
} // namespace LogCabin::Storage
} // namespace LogCabin
