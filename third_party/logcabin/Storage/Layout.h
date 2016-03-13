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

#ifndef LOGCABIN_STORAGE_LAYOUT_H
#define LOGCABIN_STORAGE_LAYOUT_H

#include "Storage/FilesystemUtil.h"

namespace LogCabin {

// forward declaration
namespace Core {
class Config;
}

namespace Storage {

/**
 * Encapsulates how LogCabin lays out the filesystem, and it handles locking of
 * the storage directory.
 *
 * The current filesystem layout looks like this:
 *
 * / - topDir, defined by config option 'storagePath'
 *     "server%lu" % serverId/ - serverDir
 *         log/ - logDir, Storage::Log implementation-defined
 *         snapshot/ -  snapshotDir, contains snapshots
 *             snapshot - latest complete snapshot
 *             "partial.%010lu.%06lu" % (seconds, micro) - in progress
 *         lock - lockFile, ensures only 1 process accesses serverDir a time
 */
class Layout {
  public:

    /// Default constructor. Call some init method next.
    Layout();

    /// Move constructor.
    Layout(Layout&& other);

    /// Destructor.
    ~Layout();

    /// Move assignment.
    Layout& operator=(Layout&& other);

    /**
     * Initialize in the normal way for a LogCabin server.
     * \param config
     *      Server settings: used to extract storage path.
     * \param serverId
     *      Unique ID for this server.
     */
    void init(const Core::Config& config, uint64_t serverId);

    /**
     * Initialize with a particular storagePath.
     * \param storagePath
     *      Path for 'topDir'.
     * \param serverId
     *      Unique ID for this server.
     */
    void init(const std::string& storagePath, uint64_t serverId);

    /**
     * Initialize for unit tests. This will set up the layout in a temporary
     * directory, and this class will remove all files in that directory when
     * it is destroyed.
     * \param serverId
     *      Unique ID for this server (in case it matters).
     */
    void initTemporary(uint64_t serverId = 1);

    /**
     * Contains all files.
     * Defined by config option 'storagePath'.
     */
    FilesystemUtil::File topDir;
    /**
     * Contains all files for this particular server.
     * Sits underneath topDir in a directory called "server%lu" % serverId.
     */
    FilesystemUtil::File serverDir;
    /**
     * Used to ensure only one process accesses serverDir at a time.
     * Sits underneath serverDir in a file called "lock".
     */
    FilesystemUtil::File lockFile;
    /**
     * Contains all log files for this particular server.
     * Sits underneath serverDir in a directory called "log".
     */
    FilesystemUtil::File logDir;
    /**
     * Contains all snapshot files for this particular server.
     * Sits underneath serverDir in a directory called "snapshot".
     */
    FilesystemUtil::File snapshotDir;

  private:
    /**
     * If true, rm -rf topDir when destroying this class.
     */
    bool removeAllFiles;

    // Layout is movable but not coypable (the semantics around removeAllFiles
    // would be tricky).
    Layout(const Layout& other) = delete;
    Layout& operator=(const Layout& other) = delete;
};

} // namespace LogCabin::Storage
} // namespace LogCabin

#endif /* LOGCABIN_STORAGE_LAYOUT_H */
