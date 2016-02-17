/* Copyright (c) 2015 Diego Ongaro
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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "Core/Config.h"
#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Storage/Layout.h"

namespace LogCabin {
namespace Storage {

namespace FS = FilesystemUtil;

Layout::Layout()
    : topDir()
    , serverDir()
    , lockFile()
    , logDir()
    , snapshotDir()
    , removeAllFiles(false)
{
}

Layout::Layout(Layout&& other)
    : topDir(std::move(other.topDir))
    , serverDir(std::move(other.serverDir))
    , lockFile(std::move(other.lockFile))
    , logDir(std::move(other.logDir))
    , snapshotDir(std::move(other.snapshotDir))
    , removeAllFiles(other.removeAllFiles)
{
    other.removeAllFiles = false;
}

Layout::~Layout()
{
    if (removeAllFiles)
        Storage::FilesystemUtil::remove(topDir.path);
}

Layout&
Layout::operator=(Layout&& other)
{
    if (removeAllFiles) {
        Storage::FilesystemUtil::remove(topDir.path);
        removeAllFiles = false;
    }
    topDir = std::move(other.topDir);
    serverDir = std::move(other.serverDir);
    lockFile = std::move(other.lockFile);
    logDir = std::move(other.logDir);
    snapshotDir = std::move(other.snapshotDir);
    removeAllFiles = other.removeAllFiles;
    other.removeAllFiles = false;
    return *this;
}

void
Layout::init(const Core::Config& config, uint64_t serverId)
{
    init(config.read<std::string>("storagePath", "storage"),
         serverId);
}

void
Layout::init(const std::string& storagePath, uint64_t serverId)
{
    if (removeAllFiles) {
        Storage::FilesystemUtil::remove(topDir.path);
        removeAllFiles = false;
    }
    topDir = FS::openDir(storagePath);
    serverDir = FS::openDir(
        topDir,
        Core::StringUtil::format("server%lu", serverId));
    // We used to lock serverDir, but that doesn't work across NFS clients, at
    // least on RHEL6. Locking a file within the directory does seem to work.
    lockFile = FS::openFile(
        serverDir,
        "lock",
        O_CREAT);
    // lock file so that Storage/Tool doesn't use serverDir while the daemon is
    // running
    std::string error = FS::tryFlock(lockFile, LOCK_EX|LOCK_NB);
    if (!error.empty()) {
        EXIT("Could not lock storage directory. Is LogCabin already running? "
             "Error was: %s", error.c_str());
    }
    logDir = FS::openDir(serverDir, "log");
    snapshotDir = FS::openDir(serverDir, "snapshot");
}

void
Layout::initTemporary(uint64_t serverId)
{
    init(Storage::FilesystemUtil::mkdtemp(), serverId);
    removeAllFiles = true;
}

} // namespace LogCabin::Storage
} // namespace LogCabin
