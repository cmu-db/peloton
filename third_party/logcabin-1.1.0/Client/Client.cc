/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014-2015 Diego Ongaro
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

#include <string.h>

#include "include/LogCabin/Client.h"
#include "Client/ClientImpl.h"
#include "Client/MockClientImpl.h"
#include "Core/StringUtil.h"

namespace LogCabin {
namespace Client {

namespace {
void throwException(const Result& result)
{
    switch (result.status) {
        case Status::OK:
            return;
        case Status::INVALID_ARGUMENT:
            throw InvalidArgumentException(result.error);
        case Status::LOOKUP_ERROR:
            throw LookupException(result.error);
        case Status::TYPE_ERROR:
            throw TypeException(result.error);
        case Status::CONDITION_NOT_MET:
            throw ConditionNotMetException(result.error);
        case Status::TIMEOUT:
            throw TimeoutException(result.error);
    }
}
} // anonymous namespace


////////// Server //////////

Server::Server(uint64_t serverId, const std::string& addresses)
    : serverId(serverId)
    , addresses(addresses)
{
}

Server::Server()
    : serverId(~0UL)
    , addresses("")
{
}

Server::Server(const Server& other)
    : serverId(other.serverId)
    , addresses(other.addresses)
{
}

Server::~Server()
{
}

Server&
Server::operator=(const Server& other)
{
    serverId = other.serverId;
    addresses = other.addresses;
    return *this;
}

////////// ConfigurationResult //////////

ConfigurationResult::ConfigurationResult()
    : status(OK)
    , badServers()
    , error()
{
}

ConfigurationResult::~ConfigurationResult()
{
}

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
        case Status::TIMEOUT:
            os << "Status::TIMEOUT";
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

////////// class Exception //////////

Exception::Exception(const std::string& error)
    : std::runtime_error(error)
{
}

InvalidArgumentException::InvalidArgumentException(const std::string& error)
    : Exception(error)
{
}

LookupException::LookupException(const std::string& error)
    : Exception(error)
{
}

TypeException::TypeException(const std::string& error)
    : Exception(error)
{
}

ConditionNotMetException::ConditionNotMetException(const std::string& error)
    : Exception(error)
{
}

TimeoutException::TimeoutException(const std::string& error)
    : Exception(error)
{
}

////////// TreeDetails //////////

/**
 * Implementation-specific members of Client::Tree.
 */
class TreeDetails {
  public:
    TreeDetails(std::shared_ptr<ClientImpl> clientImpl,
                const std::string& workingDirectory)
        : clientImpl(clientImpl)
        , workingDirectory(workingDirectory)
        , condition()
        , timeoutNanos(0)
    {
    }
    /**
     * Client implementation.
     */
    std::shared_ptr<ClientImpl> clientImpl;
    /**
     * The current working directory for the Tree (an absolute path).
     */
    std::string workingDirectory;
    /**
     * If set, specifies a predicate that must hold for operations to take
     * effect.
     */
    Condition condition;
    /**
     * If nonzero, a relative timeout in nanoseconds for all Tree operations.
     */
    uint64_t timeoutNanos;
};


////////// Tree //////////

Tree::Tree(std::shared_ptr<ClientImpl> clientImpl,
           const std::string& workingDirectory)
    : mutex()
    , treeDetails(new TreeDetails(clientImpl, workingDirectory))
{
}

Tree::Tree(const Tree& other)
    : mutex()
    , treeDetails(other.getTreeDetails())
{
}

Tree&
Tree::operator=(const Tree& other)
{
    // Hold one lock at a time to avoid deadlock and handle self-assignment.
    std::shared_ptr<const TreeDetails> otherTreeDetails =
                                            other.getTreeDetails();
    std::lock_guard<std::mutex> lockGuard(mutex);
    treeDetails = otherTreeDetails;
    return *this;
}

Result
Tree::setWorkingDirectory(const std::string& newWorkingDirectory)
{
    // This method sets the working directory regardless of whether it
    // succeeds -- that way if it doesn't, future relative paths on this Tree
    // will result in errors instead of operating on the prior working
    // directory.

    ClientImpl::TimePoint timeout =
        ClientImpl::absTimeout(treeDetails->timeoutNanos);

    std::lock_guard<std::mutex> lockGuard(mutex);
    std::string realPath;
    Result result = treeDetails->clientImpl->canonicalize(
                                newWorkingDirectory,
                                treeDetails->workingDirectory,
                                realPath);
    std::shared_ptr<TreeDetails> newTreeDetails(new TreeDetails(*treeDetails));
    if (result.status != Status::OK) {
        newTreeDetails->workingDirectory = Core::StringUtil::format(
                    "invalid from prior call to setWorkingDirectory('%s') "
                    "relative to '%s'",
                    newWorkingDirectory.c_str(),
                    treeDetails->workingDirectory.c_str());
        treeDetails = newTreeDetails;
        return result;
    }
    newTreeDetails->workingDirectory = realPath;
    treeDetails = newTreeDetails;
    return treeDetails->clientImpl->makeDirectory(realPath, "",
                                                  treeDetails->condition,
                                                  timeout);
}

void
Tree::setWorkingDirectoryEx(const std::string& workingDirectory)
{
    throwException(setWorkingDirectory(workingDirectory));
}

std::string
Tree::getWorkingDirectory() const
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->workingDirectory;
}

Result
Tree::setCondition(const std::string& path, const std::string& value)
{
    // This method sets the condition regardless of whether it succeeds -- that
    // way if it doesn't, future calls on this Tree will result in errors
    // instead of operating on the prior condition.

    std::lock_guard<std::mutex> lockGuard(mutex);
    std::string realPath;
    std::shared_ptr<TreeDetails> newTreeDetails(new TreeDetails(*treeDetails));
    if (path.empty()) {
        newTreeDetails->condition = {"", ""};
    } else {
        Result result = treeDetails->clientImpl->canonicalize(
                                    path,
                                    treeDetails->workingDirectory,
                                    realPath);
        if (result.status != Status::OK) {
            newTreeDetails->condition = {
                Core::StringUtil::format(
                        "invalid from prior call to setCondition('%s') "
                        "relative to '%s'",
                        path.c_str(),
                        treeDetails->workingDirectory.c_str()),
                value,
            };
            treeDetails = newTreeDetails;
            return result;
        }
        newTreeDetails->condition = {realPath, value};
    }
    treeDetails = newTreeDetails;
    return Result();
}

void
Tree::setConditionEx(const std::string& path, const std::string& value)
{
    throwException(setCondition(path, value));
}

std::pair<std::string, std::string>
Tree::getCondition() const
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->condition;
}

uint64_t
Tree::getTimeout() const
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->timeoutNanos;
}

void
Tree::setTimeout(uint64_t nanoseconds)
{
    std::lock_guard<std::mutex> lockGuard(mutex);
    std::shared_ptr<TreeDetails> newTreeDetails(new TreeDetails(*treeDetails));
    newTreeDetails->timeoutNanos = nanoseconds;
    treeDetails = newTreeDetails;
}

Result
Tree::makeDirectory(const std::string& path)
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->clientImpl->makeDirectory(
        path,
        treeDetails->workingDirectory,
        treeDetails->condition,
        ClientImpl::absTimeout(treeDetails->timeoutNanos));
}

void
Tree::makeDirectoryEx(const std::string& path)
{
    throwException(makeDirectory(path));
}

Result
Tree::listDirectory(const std::string& path,
                    std::vector<std::string>& children) const
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->clientImpl->listDirectory(
        path,
        treeDetails->workingDirectory,
        treeDetails->condition,
        ClientImpl::absTimeout(treeDetails->timeoutNanos),
        children);
}

std::vector<std::string>
Tree::listDirectoryEx(const std::string& path) const
{
    std::vector<std::string> children;
    throwException(listDirectory(path, children));
    return children;
}

Result
Tree::removeDirectory(const std::string& path)
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->clientImpl->removeDirectory(
        path,
        treeDetails->workingDirectory,
        treeDetails->condition,
        ClientImpl::absTimeout(treeDetails->timeoutNanos));
}

void
Tree::removeDirectoryEx(const std::string& path)
{
    throwException(removeDirectory(path));
}

Result
Tree::write(const std::string& path, const std::string& contents)
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->clientImpl->write(
        path,
        treeDetails->workingDirectory,
        contents,
        treeDetails->condition,
        ClientImpl::absTimeout(treeDetails->timeoutNanos));
}

void
Tree::writeEx(const std::string& path, const std::string& contents)
{
    throwException(write(path, contents));
}

Result
Tree::read(const std::string& path, std::string& contents) const
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->clientImpl->read(
        path,
        treeDetails->workingDirectory,
        treeDetails->condition,
        ClientImpl::absTimeout(treeDetails->timeoutNanos),
        contents);
}

std::string
Tree::readEx(const std::string& path) const
{
    std::string contents;
    throwException(read(path, contents));
    return contents;
}

Result
Tree::removeFile(const std::string& path)
{
    std::shared_ptr<const TreeDetails> treeDetails = getTreeDetails();
    return treeDetails->clientImpl->removeFile(
        path,
        treeDetails->workingDirectory,
        treeDetails->condition,
        ClientImpl::absTimeout(treeDetails->timeoutNanos));
}

void
Tree::removeFileEx(const std::string& path)
{
    throwException(removeFile(path));
}

std::shared_ptr<const TreeDetails>
Tree::getTreeDetails() const
{
    std::shared_ptr<const TreeDetails> ret;
    std::lock_guard<std::mutex> lockGuard(mutex);
    ret = treeDetails;
    return ret;
}

////////// TestingCallbacks //////////

TestingCallbacks::TestingCallbacks()
{
}

TestingCallbacks::~TestingCallbacks()
{
}

bool
TestingCallbacks::stateMachineQuery(
    Protocol::Client::StateMachineQuery_Request& request,
    Protocol::Client::StateMachineQuery_Response& response)
{
    return false;
}

bool
TestingCallbacks::stateMachineCommand(
    Protocol::Client::StateMachineCommand_Request& request,
    Protocol::Client::StateMachineCommand_Response& response)
{
    return false;
}

////////// Cluster //////////

Cluster::Cluster(std::shared_ptr<TestingCallbacks> testingCallbacks,
                 const std::map<std::string, std::string>& options)
    : clientImpl(std::make_shared<MockClientImpl>(
        testingCallbacks ? testingCallbacks
                         : std::make_shared<TestingCallbacks>()))
{
    clientImpl->init("-MOCK-");
}

Cluster::Cluster(const std::string& hosts,
                 const std::map<std::string, std::string>& options)
    : clientImpl(std::make_shared<ClientImpl>(options))
{
#if DEBUG // for testing purposes only
    if (hosts == "-MOCK-SKIP-INIT-")
        return;
#endif
    clientImpl->init(hosts);
}

Cluster::~Cluster()
{
}

std::pair<uint64_t, Configuration>
Cluster::getConfiguration() const
{
    return clientImpl->getConfiguration();
}

ConfigurationResult
Cluster::setConfiguration(uint64_t oldId,
                          const Configuration& newConfiguration)
{
    return clientImpl->setConfiguration(oldId, newConfiguration);
}


Result
Cluster::getServerInfo(const std::string& host,
                       uint64_t timeoutNanoseconds,
                       Server& info)
{
    return clientImpl->getServerInfo(
                host,
                ClientImpl::absTimeout(timeoutNanoseconds),
                info);
}

Server
Cluster::getServerInfoEx(const std::string& host,
                         uint64_t timeoutNanoseconds)
{
    Server info;
    throwException(getServerInfo(host, timeoutNanoseconds, info));
    return info;
}

Result
Cluster::getServerStats(const std::string& host,
                        uint64_t timeoutNanoseconds,
                        Protocol::ServerStats& stats)
{

    Protocol::ServerControl::ServerStatsGet::Request request;
    Protocol::ServerControl::ServerStatsGet::Response response;
    Result result = clientImpl->serverControl(
                host,
                ClientImpl::absTimeout(timeoutNanoseconds),
                Protocol::ServerControl::OpCode::SERVER_STATS_GET,
                request,
                response);
    stats = response.server_stats();
    return result;
}

Protocol::ServerStats
Cluster::getServerStatsEx(const std::string& host,
                          uint64_t timeoutNanoseconds)
{
    Protocol::ServerStats stats;
    throwException(getServerStats(host, timeoutNanoseconds, stats));
    return stats;
}

Tree
Cluster::getTree()
{
    return Tree(clientImpl, "/");
}

} // namespace LogCabin::Client
} // namespace LogCabin
