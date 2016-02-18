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

/**
 * \file
 * Contains utilities for working with the filesystem.
 */

#include <cinttypes>
#include <string>
#include <vector>

#ifndef LOGCABIN_STORAGE_FILESYSTEMUTIL_H
#define LOGCABIN_STORAGE_FILESYSTEMUTIL_H

namespace LogCabin {
namespace Storage {
namespace FilesystemUtil {

/**
 * Set to true in some unit tests to skip fsync() and fdatasync(), which can
 * speeds up some tests significantly.
 */
extern bool skipFsync;

/**
 * A File object is just a wrapper around a file descriptor; it represents
 * either an open file, an open directory, or an empty placeholder. It takes
 * charge of closing the file descriptor when it is done and tracks the path
 * used to open the file descriptor in order to provide useful error messages.
 */
class File {
  public:
    /// Default constructor.
    File();
    /// Move constructor.
    File(File&& other);
    /**
     * Constructor.
     * \param fd
     *      An open file descriptor.
     * \param path
     *      The path used to open fd; used for error messages.
     */
    File(int fd, std::string path);

    /**
     * Destructor. Closes file unless it's already been closed.
     */
    ~File();

    /// Move assignment.
    File& operator=(File&& other);

    /**
     * Close the file.
     * This object's fd and path are cleared.
     */
    void close();

    /**
     * Disassociate the file descriptor from this object.
     * The caller is in charge of closing the file descriptor. This object's fd
     * and path are cleared.
     * \return
     *      The file descriptor.
     */
    int release();

    /**
     * The open file descriptor, or -1 otherwise.
     */
    int fd;
    /**
     * The path used to open fd, or empty. Used for error messages.
     */
    std::string path;

    // File is not copyable.
    File(const File&) = delete;
    File& operator=(const File&) = delete;
};

/**
 * Allocate a contiguous range of a file, padding with zeros if necessary.
 * See man 3 posix_fallocate. Does not fsync the file.
 */
void allocate(const File& file, uint64_t offset, uint64_t bytes);

/**
 * Clones a file descriptor. See man 2 dup.
 * \param file
 *      An open file descriptor.
 * \return
 *      A copy of 'file' with a new file descriptor.
 */
File dup(const File& file);

/**
 * Flush changes to a File to its underlying storage device.
 * See #skipFsync.
 * \param file
 *      An open file descriptor.
 */
void fsync(const File& file);

/**
 * Flush changes to a File to its underlying storage device, except for
 * atime/mtime. See fdatasync man page.
 * \param file
 *      An open file descriptor.
 * See #skipFsync.
 */
void fdatasync(const File& file);

/**
 * Apply or remove an advisory lock on a file or directory.
 * See man 2 flock.
 * PANICs if any errors are encountered, even EWOULDBLOCK (see tryFlock).
 */
void flock(const File& file, int operation);

/**
 * Apply or remove an advisory lock on a file or directory.
 * See man 2 flock.
 * \param file
 *      File or directory to lock.
 * \param operation
 *      LOCK_SH, LOCK_EX, or LOCK_UN, usually ORed with LOCK_NB.
 * \return
 *      If successful, returns the empty string.
 *      If the operation would have blocked, returns a non-empty string with
 *      detailed information.
 */
std::string
tryFlock(const File& file, int operation);

/**
 * Returns the size of the file in bytes.
 */
uint64_t getSize(const File& file);

/**
 * List the contents of a directory by path.
 * Panics if the 'path' is not a directory.
 * \param path
 *      The path to the directory whose contents to list.
 * \return
 *      The names of the directory entries in the order returned by readdir.
 *      The caller will often want to prepend 'path' and a slash to these to
 *      form a path.
 */
std::vector<std::string> ls(const std::string& path);

/**
 * List the contents of an open directory.
 * Panics if 'dir' is not a directory.
 * \param dir
 *      An open file descriptor to the directory whose contents to list.
 * \return
 *      The names of the directory entries in the order returned by readdir.
 */
std::vector<std::string> ls(const File& dir);

/**
 * Open a directory, creating it if it doesn't exist.
 */
File openDir(const std::string& path);

/**
 * Open a directory relative to an already open directory, creating it if it
 * doesn't exist.
 */
File openDir(const File& dir, const std::string& child);

/**
 * Open a file. See man 2 openat.
 *
 * Panics if the file could not be opened; see #tryOpenFile() if this isn't
 * what you want.
 */
File openFile(const File& dir, const std::string& child, int flags);

/**
 * Open a file. See man 2 openat.
 *
 * Returns a default-constructed File object if the file could not be opened
 * due to EEXIST or ENOENT; see #openFile() if this isn't what you want.
 */
File tryOpenFile(const File& dir, const std::string& child, int flags);

/**
 * Remove the file or directory at path.
 * If path is a directory, its contents will also be removed.
 * If path does not exist, this returns without an error.
 * This operation is not atomic but is idempotent.
 */
void remove(const std::string& path);

/**
 * Remove the file relative to an open directory.
 * If path does not exist, this returns without an error.
 * This does not fsync the directory.
 * \param dir
 *      An open file descriptor to the directory containing path.
 * \param path
 *      The path of the file to remove, relative to dirFd.
 *      This must be a file; it may not be a directory.
 */
void
removeFile(const File& dir, const std::string& path);

/**
 * Rename a file. See man 2 renameat.
 * This does not fsync the directories.
 */
void rename(const File& oldDir, const std::string& oldChild,
            const File& newDir, const std::string& newChild);

/**
 * Open a directory, fsync it, and close it. This is useful to fsync a
 * directory after creating a file or directory within it.
 * See #skipFsync.
 */
void syncDir(const std::string& path);

/**
 * Shrink or grow a file to the specified length, padding with zeros if
 * necessary. See man 2 ftruncate. Does not fsync the file.
 */
void truncate(const File& file, uint64_t bytes);

/**
 * Return a path to a temporary directory.
 */
std::string mkdtemp();

/**
 * A wrapper around write that retries interrupted calls.
 * \param fildes
 *      The file handle on which to write data.
 * \param data
 *      A pointer to the data to write.
 * \param dataLen
 *      The number of bytes of 'data' to write.
 * \return
 *      Either -1 with errno set, or the number of bytes requested to write.
 *      This wrapper will never return -1 with errno set to EINTR.
 */
ssize_t
write(int fildes, const void* data, uint64_t dataLen);

/**
 * A wrapper around write that retries interrupted calls.
 * \param fildes
 *      The file handle on which to write data.
 * \param data
 *      An I/O vector of data to write (pointer, length pairs).
 * \return
 *      Either -1 with errno set, or the number of bytes requested to write.
 *      This wrapper will never return -1 with errno set to EINTR.
 */
ssize_t
write(int fildes,
      std::initializer_list<std::pair<const void*, uint64_t>> data);

/**
 * Provides random access to a file.
 * This implementation currently works by mmaping the file and working from the
 * in-memory copy.
 */
class FileContents {
  public:
    /**
     * Constructor.
     * \param file
     *      An open file descriptor for the file to read.
     */
    explicit FileContents(const File& file);
    /// Destructor.
    ~FileContents();
    /**
     * Return the length of the file.
     */
    uint64_t getFileLength() { return fileLen; }

    /**
     * Copy some number of bytes of the file into a user-supplied buffer.
     *
     * If there are not enough bytes in the file, this will PANIC. See
     * copyPartial if that's not what you want.
     *
     * \param offset
     *      The number of bytes into the file at which to start copying.
     * \param[out] buf
     *      The destination buffer to copy into.
     * \param length
     *      The number of bytes to copy.
     */
    void copy(uint64_t offset, void* buf, uint64_t length);

    /**
     * Copy up to some number of bytes of the file into a user-supplied buffer.
     *
     * \param offset
     *      The number of bytes into the file at which to start copying.
     * \param[out] buf
     *      The destination buffer to copy into.
     * \param maxLength
     *      The maximum number of bytes to copy.
     * \return
     *      The number of bytes copied. This can be fewer than maxLength if the
     *      file ended first.
     */
    uint64_t copyPartial(uint64_t offset, void* buf, uint64_t maxLength);

    /**
     * Get a pointer to a region of the file.
     *
     * If there are not enough bytes in the file, this will PANIC.
     *
     * \tparam T
     *      The return type is casted to a pointer of T.
     * \param offset
     *      The number of bytes into the file at which to return the pointer.
     * \param length
     *      The number of bytes that must be valid after the offset.
     * \return
     *      A pointer to a buffer containing the 'length' bytes starting at
     *      'offset' in the file. The caller may not modify this buffer. The
     *      returned buffer will remain valid until this object is destroyed.
     */
    template<typename T = void>
    const T*
    get(uint64_t offset, uint64_t length) {
        return reinterpret_cast<const T*>(getHelper(offset, length));
    }

  private:
    /// Used internally by get().
    const void* getHelper(uint64_t offset, uint64_t length);
    /// An open file descriptor for the file to read.
    File file;
    /// The number of bytes in the file.
    uint64_t fileLen;
    /// The value returned by mmap(), or NULL for empty files.
    const void* map;
    FileContents(const FileContents&) = delete;
    FileContents& operator=(const FileContents&) = delete;
};

} // namespace LogCabin::Storage::FilesystemUtil
} // namespace LogCabin::Storage
} // namespace LogCabin

#endif /* LOGCABIN_STORAGE_FILESYSTEMUTIL_H */
