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

#include <google/protobuf/message.h>
#include <memory>
#include <stdexcept>
#include <string>

#include "Core/CompatAtomic.h"
#include "Core/ProtoBuf.h"
#include "Storage/FilesystemUtil.h"

#ifndef LOGCABIN_STORAGE_SNAPSHOTFILE_H
#define LOGCABIN_STORAGE_SNAPSHOTFILE_H

namespace LogCabin {
namespace Storage {

class Layout; // forward declaration

namespace SnapshotFile {

/**
 * Remove any partial snapshots found on disk. This is normally called when the
 * server boots up.
 */
void discardPartialSnapshots(const Storage::Layout& storageLayout);

/**
 * Assists in reading snapshot files from the local filesystem.
 */
class Reader : public Core::ProtoBuf::InputStream {
  public:
    /**
     * Constructor.
     * \param storageLayout
     *      The directories in which to find the snapshot (in a file called
     *      "snapshot" in the snapshotDir).
     * \throw std::runtime_error
     *      If the file can't be found.
     */
    explicit Reader(const Storage::Layout& storageLayout);
    /// Destructor.
    ~Reader();
    /// Return the size in bytes for the file.
    uint64_t getSizeBytes();
    // See Core::ProtoBuf::InputStream.
    uint64_t getBytesRead() const;
    // See Core::ProtoBuf::InputStream.
    std::string readMessage(google::protobuf::Message& message);
    // See Core::ProtoBuf::InputStream.
    uint64_t readRaw(void* data, uint64_t length);
  private:
    /// Wraps the raw file descriptor; in charge of closing it when done.
    Storage::FilesystemUtil::File file;
    /// Maps the file into memory for reading.
    std::unique_ptr<Storage::FilesystemUtil::FileContents> contents;
    /// The number of bytes read from the file.
    uint64_t bytesRead;
};

/**
 * Assists in writing snapshot files to the local filesystem.
 */
class Writer : public Core::ProtoBuf::OutputStream {
  public:
    /**
     * Allocates an object that is shared across processes. Uses a shared,
     * anonymous mmap region internally.
     */
    template<typename T>
    struct SharedMMap {
        SharedMMap();
        ~SharedMMap();
        T* value; // pointer does not change after construction
        // SharedMMap is not copyable
        SharedMMap(const SharedMMap& other) = delete;
        SharedMMap& operator=(const SharedMMap& other) = delete;
    };

    /**
     * Constructor.
     * \param storageLayout
     *      The directories in which to create the snapshot (in a file called
     *      "snapshot" in the snapshotDir).
     * TODO(ongaro): what if it can't be written?
     */
    explicit Writer(const Storage::Layout& storageLayout);
    /**
     * Destructor.
     * If the file hasn't been explicitly saved or discarded, prints a warning
     * and discards the file.
     */
    ~Writer();
    /**
     * Throw away the file.
     * If you call this after the file has been closed, it will PANIC.
     */
    void discard();
    /**
     * Flush changes just down to the operating system's buffer cache.
     * Leave the file open for additional writes.
     *
     * This is useful when forking child processes to write to the file.
     * The correct procedure for that is:
     *  0. write stuff
     *  1. call flushToOS()
     *  2. fork
     *  3. child process: write stuff
     *  4. child process: call flushToOS()
     *  5. child process: call _exit()
     *  6. parent process: call seekToEnd()
     *  7. parent process: write stuff
     *  8. parent process: call save()
     */
    void flushToOS();
    /**
     * Seek to the end of the file, in case another process has written to it.
     * Subsequent calls to getBytesWritten() will include data written by other
     * processes.
     */
    void seekToEnd();
    /**
     * Flush changes all the way down to the disk and close the file.
     * If you call this after the file has been closed, it will PANIC.
     * \return
     *      Size in bytes of the file
     */
    uint64_t save();
    // See Core::ProtoBuf::OutputStream.
    uint64_t getBytesWritten() const;
    // See Core::ProtoBuf::OutputStream.
    void writeMessage(const google::protobuf::Message& message);
    // See Core::ProtoBuf::OutputStream.
    void writeRaw(const void* data, uint64_t length);

  private:
    /// A handle to the directory containing the snapshot. Used for renameat on
    /// close.
    Storage::FilesystemUtil::File parentDir;
    /// The temporary name of 'file' before it is closed.
    std::string stagingName;
    /// Wraps the raw file descriptor; in charge of closing it when done.
    Storage::FilesystemUtil::File file;
    /// The number of bytes accumulated in the file so far.
    uint64_t bytesWritten;
  public:
    /**
     * This value is incremented every time bytes are written to the Writer
     * from any process holding this Writer. Used by Server/StateMachine to
     * implement a watchdog that checks progress of a snapshotting process.
     */
    SharedMMap<std::atomic<uint64_t>> sharedBytesWritten;

};

} // namespace LogCabin::Storage::SnapshotFile
} // namespace LogCabin::Storage
} // namespace LogCabin

#endif /* LOGCABIN_STORAGE_SNAPSHOTFILE_H */
