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

#include <cassert>

#include "build/Protocol/ServerStats.pb.h"
#include "build/Tree/Snapshot.pb.h"
#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Tree/Tree.h"

namespace LogCabin {
namespace Tree {

using Core::StringUtil::format;
using namespace Internal; // NOLINT

////////// enum Status //////////

std::ostream&
operator<<(std::ostream& os, Status status)
{
    switch (status) {
        case Status::OK:
            os << "Status::OK";
            break;
        case Status::INVALID_ARGUMENT:
            os << "Status::INVALID_ARGUMENT";
            break;
        case Status::LOOKUP_ERROR:
            os << "Status::LOOKUP_ERROR";
            break;
        case Status::TYPE_ERROR:
            os << "Status::TYPE_ERROR";
            break;
        case Status::CONDITION_NOT_MET:
            os << "Status::CONDITION_NOT_MET";
            break;
    }
    return os;
}

////////// struct Result //////////

Result::Result()
    : status(Status::OK)
    , error()
{
}

namespace Internal {

////////// class File //////////

File::File()
    : contents()
{
}

void
File::dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const
{
    Snapshot::File file;
    file.set_contents(contents);
    stream.writeMessage(file);
}

void
File::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
    Snapshot::File node;
    std::string error = stream.readMessage(node);
    if (!error.empty()) {
        PANIC("Couldn't read snapshot: %s", error.c_str());
    }
    contents = node.contents();
}

////////// class Directory //////////

Directory::Directory()
    : directories()
    , files()
{
}

std::vector<std::string>
Directory::getChildren() const
{
    std::vector<std::string> children;
    for (auto it = directories.begin(); it != directories.end(); ++it)
        children.push_back(it->first + "/");
    for (auto it = files.begin(); it != files.end(); ++it)
        children.push_back(it->first);
    return children;
}

Directory*
Directory::lookupDirectory(const std::string& name)
{
    return const_cast<Directory*>(
        const_cast<const Directory*>(this)->lookupDirectory(name));
}

const Directory*
Directory::lookupDirectory(const std::string& name) const
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    auto it = directories.find(name);
    if (it == directories.end())
        return NULL;
    return &it->second;
}


Directory*
Directory::makeDirectory(const std::string& name)
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    if (lookupFile(name) != NULL)
        return NULL;
    return &directories[name];
}

void
Directory::removeDirectory(const std::string& name)
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    directories.erase(name);
}

File*
Directory::lookupFile(const std::string& name)
{
    return const_cast<File*>(
        const_cast<const Directory*>(this)->lookupFile(name));
}

const File*
Directory::lookupFile(const std::string& name) const
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    auto it = files.find(name);
    if (it == files.end())
        return NULL;
    return &it->second;
}

File*
Directory::makeFile(const std::string& name)
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    if (lookupDirectory(name) != NULL)
        return NULL;
    return &files[name];
}

bool
Directory::removeFile(const std::string& name)
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    return (files.erase(name) > 0);
}

void
Directory::dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const
{
    // create protobuf of this dir, listing all children
    Snapshot::Directory dir;
    for (auto it = directories.begin(); it != directories.end(); ++it)
        dir.add_directories(it->first);
    for (auto it = files.begin(); it != files.end(); ++it)
        dir.add_files(it->first);

    // write dir into stream
    stream.writeMessage(dir);

    // dump children in the same order
    for (auto it = directories.begin(); it != directories.end(); ++it)
        it->second.dumpSnapshot(stream);
    for (auto it = files.begin(); it != files.end(); ++it)
        it->second.dumpSnapshot(stream);
}

void
Directory::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
    Snapshot::Directory dir;
    std::string error = stream.readMessage(dir);
    if (!error.empty()) {
        PANIC("Couldn't read snapshot: %s", error.c_str());
    }
    for (auto it = dir.directories().begin();
         it != dir.directories().end();
         ++it) {
        directories[*it].loadSnapshot(stream);
    }
    for (auto it = dir.files().begin();
         it != dir.files().end();
         ++it) {
        files[*it].loadSnapshot(stream);
    }
}

////////// class Path //////////

Path::Path(const std::string& symbolic)
    : result()
    , symbolic(symbolic)
    , parents()
    , target()
{
    if (!Core::StringUtil::startsWith(symbolic, "/")) {
        result.status = Status::INVALID_ARGUMENT;
        result.error = format("'%s' is not a valid path",
                              symbolic.c_str());
        return;
    }

    // Add /root prefix (see docs for Tree::superRoot)
    parents.push_back("root");

    // Split the path into a list of parent components and a target.
    std::string word;
    for (auto it = symbolic.begin(); it != symbolic.end(); ++it) {
        if (*it == '/') {
            if (!word.empty()) {
                parents.push_back(word);
                word.clear();
            }
        } else {
            word += *it;
        }
    }
    if (!word.empty())
        parents.push_back(word);
    target = parents.back();
    parents.pop_back();
}

std::string
Path::parentsThrough(std::vector<std::string>::const_iterator end) const
{
    auto it = parents.begin();
    ++it; // skip "root"
    ++end; // end was inclusive, now exclusive
    if (it == end)
        return "/";
    std::string ret;
    do {
        ret += "/" + *it;
        ++it;
    } while (it != end);
    return ret;
}

} // LogCabin::Tree::Internal

////////// class Tree //////////

Tree::Tree()
    : superRoot()
    , numConditionsChecked(0)
    , numConditionsFailed(0)
    , numMakeDirectoryAttempted(0)
    , numMakeDirectorySuccess(0)
    , numListDirectoryAttempted(0)
    , numListDirectorySuccess(0)
    , numRemoveDirectoryAttempted(0)
    , numRemoveDirectoryParentNotFound(0)
    , numRemoveDirectoryTargetNotFound(0)
    , numRemoveDirectoryDone(0)
    , numRemoveDirectorySuccess(0)
    , numWriteAttempted(0)
    , numWriteSuccess(0)
    , numReadAttempted(0)
    , numReadSuccess(0)
    , numRemoveFileAttempted(0)
    , numRemoveFileParentNotFound(0)
    , numRemoveFileTargetNotFound(0)
    , numRemoveFileDone(0)
    , numRemoveFileSuccess(0)
{
    // Create the root directory so that users don't have to explicitly
    // call makeDirectory("/").
    superRoot.makeDirectory("root");
}

Result
Tree::normalLookup(const Path& path, Directory** parent)
{
    return normalLookup(path,
                        const_cast<const Directory**>(parent));
}

Result
Tree::normalLookup(const Path& path, const Directory** parent) const
{
    *parent = NULL;
    Result result;
    const Directory* current = &superRoot;
    for (auto it = path.parents.begin(); it != path.parents.end(); ++it) {
        const Directory* next = current->lookupDirectory(*it);
        if (next == NULL) {
            if (current->lookupFile(*it) == NULL) {
                result.status = Status::LOOKUP_ERROR;
                result.error = format("Parent %s of %s does not exist",
                                      path.parentsThrough(it).c_str(),
                                      path.symbolic.c_str());
            } else {
                result.status = Status::TYPE_ERROR;
                result.error = format("Parent %s of %s is a file",
                                      path.parentsThrough(it).c_str(),
                                      path.symbolic.c_str());
            }
            return result;
        }
        current = next;
    }
    *parent = current;
    return result;
}

Result
Tree::mkdirLookup(const Path& path, Directory** parent)
{
    *parent = NULL;
    Result result;
    Directory* current = &superRoot;
    for (auto it = path.parents.begin(); it != path.parents.end(); ++it) {
        Directory* next = current->makeDirectory(*it);
        if (next == NULL) {
            result.status = Status::TYPE_ERROR;
            result.error = format("Parent %s of %s is a file",
                                  path.parentsThrough(it).c_str(),
                                  path.symbolic.c_str());
            return result;
        }
        current = next;
    }
    *parent = current;
    return result;
}

void
Tree::dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const
{
    superRoot.dumpSnapshot(stream);
}

/**
 * Load the tree from the given stream.
 */
void
Tree::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
    superRoot = Directory();
    superRoot.loadSnapshot(stream);
}


Result
Tree::checkCondition(const std::string& path,
                     const std::string& contents) const
{
    ++numConditionsChecked;
    std::string actualContents;
    Result readResult = read(path, actualContents);
    if (readResult.status == Status::OK) {
        if (contents == actualContents) {
            return Result();
        } else {
            Result result;
            result.status = Status::CONDITION_NOT_MET;
            result.error = format("Path '%s' has value '%s', not '%s' as "
                                  "required",
                                  path.c_str(),
                                  actualContents.c_str(),
                                  contents.c_str());
            ++numConditionsFailed;
            return result;
        }
    }
    if (readResult.status == Status::LOOKUP_ERROR && contents.empty()) {
        return Result();
    }
    Result result;
    result.status = Status::CONDITION_NOT_MET;
    result.error = format("Could not read value at path '%s': %s",
                          path.c_str(), readResult.error.c_str());
    ++numConditionsFailed;
    return result;
}

Result
Tree::makeDirectory(const std::string& symbolicPath)
{
    ++numMakeDirectoryAttempted;
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = mkdirLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    if (parent->makeDirectory(path.target) == NULL) {
        result.status = Status::TYPE_ERROR;
        result.error = format("%s already exists but is a file",
                              path.symbolic.c_str());
        return result;
    }
    ++numMakeDirectorySuccess;
    return result;
}

Result
Tree::listDirectory(const std::string& symbolicPath,
                    std::vector<std::string>& children) const
{
    ++numListDirectoryAttempted;
    children.clear();
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    const Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    const Directory* targetDir = parent->lookupDirectory(path.target);
    if (targetDir == NULL) {
        if (parent->lookupFile(path.target) == NULL) {
            result.status = Status::LOOKUP_ERROR;
            result.error = format("%s does not exist",
                                  path.symbolic.c_str());
        } else {
            result.status = Status::TYPE_ERROR;
            result.error = format("%s is a file",
                                  path.symbolic.c_str());
        }
        return result;
    }
    children = targetDir->getChildren();
    ++numListDirectorySuccess;
    return result;
}

Result
Tree::removeDirectory(const std::string& symbolicPath)
{
    ++numRemoveDirectoryAttempted;
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status == Status::LOOKUP_ERROR) {
        // no parent, already done
        ++numRemoveDirectoryParentNotFound;
        ++numRemoveDirectorySuccess;
        return Result();
    }
    if (result.status != Status::OK)
        return result;
    Directory* targetDir = parent->lookupDirectory(path.target);
    if (targetDir == NULL) {
        if (parent->lookupFile(path.target)) {
            result.status = Status::TYPE_ERROR;
            result.error = format("%s is a file",
                                  path.symbolic.c_str());
            return result;
        } else {
            // target does not exist, already done
            ++numRemoveDirectoryTargetNotFound;
            ++numRemoveDirectorySuccess;
            return result;
        }
    }
    parent->removeDirectory(path.target);
    if (parent == &superRoot) { // removeDirectory("/")
        // If the caller is trying to remove the root directory, we remove the
        // contents but not the directory itself. The easiest way to do this
        // is to drop but then recreate the directory.
        parent->makeDirectory(path.target);
    }
    ++numRemoveDirectoryDone;
    ++numRemoveDirectorySuccess;
    return result;
}

Result
Tree::write(const std::string& symbolicPath, const std::string& contents)
{
    ++numWriteAttempted;
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    File* targetFile = parent->makeFile(path.target);
    if (targetFile == NULL) {
        result.status = Status::TYPE_ERROR;
        result.error = format("%s is a directory",
                              path.symbolic.c_str());
        return result;
    }
    targetFile->contents = contents;
    ++numWriteSuccess;
    return result;
}

Result
Tree::read(const std::string& symbolicPath, std::string& contents) const
{
    ++numReadAttempted;
    contents.clear();
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    const Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    const File* targetFile = parent->lookupFile(path.target);
    if (targetFile == NULL) {
        if (parent->lookupDirectory(path.target) != NULL) {
            result.status = Status::TYPE_ERROR;
            result.error = format("%s is a directory",
                                  path.symbolic.c_str());
        } else {
            result.status = Status::LOOKUP_ERROR;
            result.error = format("%s does not exist",
                                  path.symbolic.c_str());
        }
        return result;
    }
    contents = targetFile->contents;
    ++numReadSuccess;
    return result;
}

Result
Tree::removeFile(const std::string& symbolicPath)
{
    ++numRemoveFileAttempted;
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status == Status::LOOKUP_ERROR) {
        // no parent, already done
        ++numRemoveFileParentNotFound;
        ++numRemoveFileSuccess;
        return Result();
    }
    if (result.status != Status::OK)
        return result;
    if (parent->lookupDirectory(path.target) != NULL) {
        result.status = Status::TYPE_ERROR;
        result.error = format("%s is a directory",
                              path.symbolic.c_str());
        return result;
    }
    if (parent->removeFile(path.target))
        ++numRemoveFileDone;
    else
        ++numRemoveFileTargetNotFound;
    ++numRemoveFileSuccess;
    return result;
}

void
Tree::updateServerStats(Protocol::ServerStats::Tree& tstats) const
{
    tstats.set_num_conditions_checked(
         numConditionsChecked);
    tstats.set_num_conditions_failed(
        numConditionsFailed);
    tstats.set_num_make_directory_attempted(
        numMakeDirectoryAttempted);
    tstats.set_num_make_directory_success(
        numMakeDirectorySuccess);
    tstats.set_num_list_directory_attempted(
        numListDirectoryAttempted);
    tstats.set_num_list_directory_success(
        numListDirectorySuccess);
    tstats.set_num_remove_directory_attempted(
        numRemoveDirectoryAttempted);
    tstats.set_num_remove_directory_parent_not_found(
        numRemoveDirectoryParentNotFound);
    tstats.set_num_remove_directory_target_not_found(
        numRemoveDirectoryTargetNotFound);
    tstats.set_num_remove_directory_done(
        numRemoveDirectoryDone);
    tstats.set_num_remove_directory_success(
        numRemoveDirectorySuccess);
    tstats.set_num_write_attempted(
        numWriteAttempted);
    tstats.set_num_write_success(
        numWriteSuccess);
    tstats.set_num_read_attempted(
        numReadAttempted);
    tstats.set_num_read_success(
        numReadSuccess);
    tstats.set_num_remove_file_attempted(
        numRemoveFileAttempted);
    tstats.set_num_remove_file_parent_not_found(
        numRemoveFileParentNotFound);
    tstats.set_num_remove_file_target_not_found(
        numRemoveFileTargetNotFound);
    tstats.set_num_remove_file_done(
        numRemoveFileDone);
    tstats.set_num_remove_file_success(
        numRemoveFileSuccess);
}

} // namespace LogCabin::Tree
} // namespace LogCabin
