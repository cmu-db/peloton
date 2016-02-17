/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014 Diego Ongaro
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

#include <map>
#include <string>
#include <vector>

#include "Core/ProtoBuf.h"

#ifndef LOGCABIN_TREE_TREE_H
#define LOGCABIN_TREE_TREE_H

namespace LogCabin {

// forward declaration
namespace Protocol {
class ServerStats_Tree;
}

namespace Tree {

/**
 * Status codes returned by Tree operations.
 */
enum class Status {

    /**
     * The operation completed successfully.
     */
    OK = 0,

    /**
     * If an argument is malformed (for example, a path that does not start
     * with a slash).
     */
    INVALID_ARGUMENT = 1,

    /**
     * If a file or directory that is required for the operation does not
     * exist.
     */
    LOOKUP_ERROR = 2,

    /**
     * If a directory exists where a file is required or a file exists where
     * a directory is required. 
     */
    TYPE_ERROR = 3,

    /**
     * A predicate on an operation was not satisfied.
     */
    CONDITION_NOT_MET = 4,
};

/**
 * Print a status code to a stream.
 */
std::ostream&
operator<<(std::ostream& os, Status status);

/**
 * Returned by Tree operations; contain a status code and an error message.
 */
struct Result {
    /**
     * Default constructor. Sets status to OK and error to the empty string.
     */
    Result();
    /**
     * A code for whether an operation succeeded or why it did not. This is
     * meant to be used programmatically.
     */
    Status status;
    /**
     * If status is not OK, this is a human-readable message describing what
     * went wrong.
     */
    std::string error;
};

namespace Internal {

/**
 * A leaf object in the Tree; stores an opaque blob of data.
 */
class File {
  public:
    /// Default constructor.
    File();
    /**
     * Write the file to the stream.
     */
    void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const;
    /**
     * Load the file from the stream.
     */
    void loadSnapshot(Core::ProtoBuf::InputStream& stream);
    /**
     * Opaque data stored in the File.
     */
    std::string contents;
};

/**
 * An interior object in the Tree; stores other Directories and Files.
 * Pointers returned by this class are valid until the File or Directory they
 * refer to is removed.
 */
class Directory {
  public:
    /// Default constructor.
    Directory();

    /**
     * List the contents of the directory.
     * \return
     *      The names of the directories and files that this directory
     *      immediately contains. The names of directories in this listing will
     *      have a trailing slash. The order is first directories (sorted
     *      lexicographically), then files (sorted lexicographically).
     */
    std::vector<std::string> getChildren() const;

    /**
     * Find the child directory by the given name.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      The directory by the given name, or
     *      NULL if it is not found or a file exists by that name.
     */
    Directory* lookupDirectory(const std::string& name);
    /**
     * Find the child directory by the given name (const version).
     * \copydetails lookupDirectory
     */
    const Directory* lookupDirectory(const std::string& name) const;
    /**
     * Find the child directory by the given name, or create it if it doesn't
     * exist.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      The directory by the given name, or
     *      NULL if a file exists by that name.
     */
    Directory* makeDirectory(const std::string& name);
    /**
     * Remove the child directory by the given name, if any. This will remove
     * all the contents of the directory as well.
     * \param name
     *      Must not contain a trailing slash.
     */
    void removeDirectory(const std::string& name);

    /**
     * Find the child file by the given name.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      The file by the given name, or
     *      NULL if it is not found or a directory exists by that name.
     */
    File* lookupFile(const std::string& name);
    /**
     * Find the child file by the given name (const version).
     * \copydetails lookupFile
     */
    const File* lookupFile(const std::string& name) const;
    /**
     * Find the child file by the given name, or create it if it doesn't exist.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      The file by the given name, or
     *      NULL if a directory exists by that name.
     */
    File* makeFile(const std::string& name);
    /**
     * Remove the child file by the given name, if any.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      True if child file removed, false if no such file existed. This is
     *      mostly useful for counting statistics.
     */
    bool removeFile(const std::string& name);

    /**
     * Write the directory and its children to the stream.
     */
    void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const;
    /**
     * Load the directory and its children from the stream.
     */
    void loadSnapshot(Core::ProtoBuf::InputStream& stream);

  private:
    /**
     * Map from names of child directories (without trailing slashes) to the
     * Directory objects.
     */
    std::map<std::string, Directory> directories;
    /**
     * Map from names of child files to the File objects.
     */
    std::map<std::string, File> files;
};

/**
 * This is used by Tree to parse symbolic paths into their components.
 */
class Path {
  public:
    /**
     * Constructor.
     * \param symbolic
     *      A path delimited by slashes. This must begin with a slash.
     *      (It should not include "/root" to arrive at the root directory.)
     * \warning
     *      The caller must check "result" to see if the path was parsed
     *      successfully.
     */
    explicit Path(const std::string& symbolic);

    /**
     * Used to generate error messages during path lookup.
     * \param end
     *      The last component of 'parents' to include in the returned string;
     *      this is typically the component that caused an error in path
     *      traversal.
     * \return
     *      The prefix of 'parents' up to and including the given end position.
     *      This is returned as a slash-delimited string not including "/root".
     */
    std::string
    parentsThrough(std::vector<std::string>::const_iterator end) const;

  public:
    /**
     * Status and error message from the constructor. Possible errors are:
     * - INVALID_ARGUMENT if path is malformed.
     */
    Result result;

    /**
     * The exact argument given to the constructor.
     */
    std::string symbolic;
    /**
     * The directories needed to traverse to get to the target.
     * This usually begins with "root" to get from the super root to the root
     * directory, then includes the components of the symbolic path up to but
     * not including the target. If the symbolic path is "/", this will be
     * empty.
     */
    std::vector<std::string> parents;
    /**
     * The final component of the path.
     * This is usually at the end of the symbolic path. If the symbolic path is
     * "/", this will be "root", used to get from the super root to the root
     * directory.
     */
    std::string target;
};

} // LogCabin::Tree::Internal

/**
 * This is an in-memory, hierarchical key-value store.
 * TODO(ongaro): Document how this fits into the rest of the system.
 */
class Tree {
  public:
    /**
     * Constructor.
     */
    Tree();

    /**
     * Write the tree to the given stream.
     */
    void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const;

    /**
     * Load the tree from the given stream.
     * \warning
     *      This will blow away any existing files and directories.
     */
    void loadSnapshot(Core::ProtoBuf::InputStream& stream);

    /**
     * Verify that the file at path has the given contents.
     * \param path
     *      The path to the file that must have the contents specified in
     *      'contents'.
     * \param contents
     *      The contents that the file specified by 'path' should have for an
     *      OK response. An OK response is also returned if 'contents' is the
     *      empty string and the file specified by 'path' does not exist.
     * \return
     *      Status and error message. Possible errors are:
     *       - CONDITION_NOT_MET upon any error.
     */
    Result
    checkCondition(const std::string& path,
                   const std::string& contents) const;

    /**
     * Make sure a directory exists at the given path.
     * Create parent directories listed in path as necessary.
     * \param path
     *      The path where there should be a directory after this call.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a file.
     */
    Result
    makeDirectory(const std::string& path);

    /**
     * List the contents of a directory.
     * \param path
     *      The directory whose direct children to list.
     * \param[out] children
     *      This will be replaced by a listing of the names of the directories
     *      and files that the directory at 'path' immediately contains. The
     *      names of directories in this listing will have a trailing slash.
     *      The order is first directories (sorted lexicographically), then
     *      files (sorted lexicographically).
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - LOOKUP_ERROR if a parent of path does not exist.
     *       - LOOKUP_ERROR if path does not exist.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a file.
     */
    Result
    listDirectory(const std::string& path,
                  std::vector<std::string>& children) const;

    /**
     * Make sure a directory does not exist.
     * Also removes all direct and indirect children of the directory.
     *
     * If called with the root directory, this will remove all descendants but
     * not actually remove the root directory; it will still return status OK.
     *
     * \param path
     *      The path where there should not be a directory after this call.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a file.
     */
    Result
    removeDirectory(const std::string& path);

    /**
     * Set the value of a file.
     * \param path
     *      The path where there should be a file with the given contents after
     *      this call.
     * \param contents
     *      The new value associated with the file.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - INVALID_ARGUMENT if contents are too large to fit in a file.
     *       - LOOKUP_ERROR if a parent of path does not exist.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a directory.
     */
    Result
    write(const std::string& path, const std::string& contents);

    /**
     * Get the value of a file.
     * \param path
     *      The path of the file whose contents to read.
     * \param contents
     *      The current value associated with the file.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - LOOKUP_ERROR if a parent of path does not exist.
     *       - LOOKUP_ERROR if path does not exist.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path is a directory.
     */
    Result
    read(const std::string& path, std::string& contents) const;

    /**
     * Make sure a file does not exist.
     * \param path
     *      The path where there should not be a file after this call.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a directory.
     */
    Result
    removeFile(const std::string& path);

    /**
     * Add metrics about the tree to the given structure.
     */
    void
    updateServerStats(Protocol::ServerStats_Tree& tstats) const;

  private:
    /**
     * Resolve the final next-to-last component of the given path (the target's
     * parent).
     * \param[in] path
     *      The path whose parent directory to find.
     * \param[out] parent
     *      Upon successful return, points to the target's parent directory.
     * \return
     *      Status and error message. Possible errors are:
     *       - LOOKUP_ERROR if a parent of path does not exist.
     *       - TYPE_ERROR if a parent of path is a file.
     */
    Result
    normalLookup(const Internal::Path& path,
                 Internal::Directory** parent);

    /**
     * Resolve the final next-to-last component of the given path (the target's
     * parent) -- const version.
     * \copydetails normalLookup
     */
    Result
    normalLookup(const Internal::Path& path,
                 const Internal::Directory** parent) const;

    /**
     * Like normalLookup but creates parent directories as necessary.
     * \param[in] path
     *      The path whose parent directory to find.
     * \param[out] parent
     *      Upon successful return, points to the target's parent directory.
     * \return
     *      Status and error message. Possible errors are:
     *       - TYPE_ERROR if a parent of path is a file.
     */
    Result
    mkdirLookup(const Internal::Path& path, Internal::Directory** parent);

    /**
     * This directory contains the root directory. The super root has a single
     * child directory named "root", and the rest of the tree lies below
     * "root". This is just an implementation detail; this class prepends
     * "/root" to every path provided by the caller.
     *
     * This removes a lot of special-case branches because every operation now
     * has a name of a target within a parent directory -- even those operating
     * on the root directory.
     */
    Internal::Directory superRoot;

    // Server stats collected in updateServerStats.
    // Note that when a condition fails, the operation is not invoked,
    // so operations whose conditions fail are not counted as 'Attempted'.
    mutable uint64_t numConditionsChecked;
    mutable uint64_t numConditionsFailed;
    uint64_t numMakeDirectoryAttempted;
    uint64_t numMakeDirectorySuccess;
    mutable uint64_t numListDirectoryAttempted;
    mutable uint64_t numListDirectorySuccess;
    uint64_t numRemoveDirectoryAttempted;
    uint64_t numRemoveDirectoryParentNotFound;
    uint64_t numRemoveDirectoryTargetNotFound;
    uint64_t numRemoveDirectoryDone;
    uint64_t numRemoveDirectorySuccess;
    uint64_t numWriteAttempted;
    uint64_t numWriteSuccess;
    mutable uint64_t numReadAttempted;
    mutable uint64_t numReadSuccess;
    uint64_t numRemoveFileAttempted;
    uint64_t numRemoveFileParentNotFound;
    uint64_t numRemoveFileTargetNotFound;
    uint64_t numRemoveFileDone;
    uint64_t numRemoveFileSuccess;
};


} // namespace LogCabin::Tree
} // namespace LogCabin

#endif // LOGCABIN_TREE_TREE_H
