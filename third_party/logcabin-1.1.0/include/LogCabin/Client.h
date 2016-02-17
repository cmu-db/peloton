/* Copyright (c) 2011-2014 Stanford University
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

/**
 * \file
 * This file declares the interface for LogCabin's client library.
 */

#include <cstddef>
#include <memory>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#ifndef LOGCABIN_INCLUDE_LOGCABIN_CLIENT_H
#define LOGCABIN_INCLUDE_LOGCABIN_CLIENT_H

namespace LogCabin {

// forward declarations
namespace Core {
namespace Debug {
} // namespace LogCabin::Core::Debug
} // namespace LogCabin::Core

// forward declarations
namespace Protocol {
class ServerStats;
namespace Client {
class StateMachineQuery_Request;
class StateMachineQuery_Response;
class StateMachineCommand_Request;
class StateMachineCommand_Response;
} // namespace LogCabin::Protocol::Client
} // namespace LogCabin::Protocol

namespace Client {

class ClientImpl; // forward declaration
class TreeDetails; // forward declaration

// To control how the debug log operates, clients should
// #include <LogCabin/Debug.h>
// and access it through
// LogCabin::Client::Debug.
namespace Debug = Core::Debug;

/**
 * A member of the cluster Configuration.
 */
struct Server {
    /// Constructor.
    Server(uint64_t serverId, const std::string& addresses);
    /// Default constructor.
    Server();
    /// Copy constructor.
    Server(const Server& other);
    /// Destructor.
    ~Server();
    /// Copy assignment.
    Server& operator=(const Server& other);

    /**
     * The unique ID of the server.
     */
    uint64_t serverId;

    /**
     * The network addresses of the server (comma-delimited).
     */
    std::string addresses;
};

/**
 * Defines the members of the cluster.
 * Used in Cluster::getConfiguration and Cluster::setConfiguration.
 */
typedef std::vector<Server> Configuration;

/**
 * Returned by Cluster::setConfiguration.
 */
struct ConfigurationResult {
    ConfigurationResult();
    ~ConfigurationResult();

    enum Status {
        /**
         * The operation succeeded.
         */
        OK = 0,
        /**
         * The supplied 'oldId' is no longer current.
         * Call GetConfiguration, re-apply your changes, and try again.
         */
        CHANGED = 1,
        /**
         * The reconfiguration was aborted because some servers are
         * unavailable.
         */
        BAD = 2,
    } status;

    /**
     * If status is BAD, the servers that were unavailable to join the cluster.
     */
    Configuration badServers;

    /**
     * Error message, if status is not OK.
     */
    std::string error;
};

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
     * A predicate which was previously set on operations with
     * Tree::setCondition() was not satisfied.
     */
    CONDITION_NOT_MET = 4,

    /**
     * A timeout specified by Tree::setTimeout() elapsed while waiting for
     * an operation to complete. It is not known whether the operation has or
     * will complete, only that a positive acknowledgment was not received
     * before the timeout elapsed.
     */
    TIMEOUT = 5,
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

/**
 * Base class for LogCabin client exceptions.
 */
class Exception : public std::runtime_error {
  public:
    explicit Exception(const std::string& error);
};

/**
 * See Status::INVALID_ARGUMENT.
 */
class InvalidArgumentException : public Exception {
  public:
    explicit InvalidArgumentException(const std::string& error);
};

/**
 * See Status::LOOKUP_ERROR.
 */
class LookupException : public Exception {
  public:
    explicit LookupException(const std::string& error);
};

/**
 * See Status::TYPE_ERROR.
 */
class TypeException : public Exception {
  public:
    explicit TypeException(const std::string& error);
};

/**
 * See Status::CONDITION_NOT_MET.
 */
class ConditionNotMetException : public Exception {
  public:
    explicit ConditionNotMetException(const std::string& error);
};

/**
 * See Status::TIMEOUT.
 */
class TimeoutException : public Exception {
  public:
    explicit TimeoutException(const std::string& error);
};

/**
 * Provides access to the hierarchical key-value store.
 * You can get an instance of Tree through Cluster::getTree() or by copying
 * an existing Tree.
 *
 * A Tree has a working directory from which all relative paths (those that do
 * not begin with a '/' are resolved). This allows different applications and
 * modules to conveniently access their own subtrees -- they can have their own
 * Tree instances and set their working directories accordingly.
 *
 * Methods that can fail come in two flavors. The first flavor returns Result
 * values with error codes and messages; the second throws exceptions upon
 * errors. These can be distinguished by the "Ex" suffix in the names of
 * methods that throw exceptions.
 */
class Tree {
  private:
    /// Constructor.
    Tree(std::shared_ptr<ClientImpl> clientImpl,
         const std::string& workingDirectory);
  public:
    /// Copy constructor.
    Tree(const Tree& other);
    /// Assignment operator.
    Tree& operator=(const Tree& other);

    /**
     * Set the working directory for this object. This directory will be
     * created if it does not exist.
     * \param workingDirectory
     *      The new working directory, which may be relative to the current
     *      working directory.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if workingDirectory is malformed.
     *       - TYPE_ERROR if parent of workingDirectory is a file.
     *       - TYPE_ERROR if workingDirectory exists but is a file.
     *       - CONDITION_NOT_MET if predicate from setCondition() was false.
     *       - TIMEOUT if timeout elapsed before the operation completed.
     *      If this returns an error, future operations on this tree using
     *      relative paths will fail until a valid working directory is set.
     */
    Result setWorkingDirectory(const std::string& workingDirectory);

    /**
     * Like setWorkingDirectory but throws exceptions upon errors.
     */
    void setWorkingDirectoryEx(const std::string& workingDirectory);

    /**
     * Return the working directory for this object.
     * \return
     *      An absolute path that is the prefix for relative paths used with
     *      this Tree object.
     */
    std::string getWorkingDirectory() const;

    /**
     * Return the condition set by a previous call to setCondition().
     * \return
     *      First component: the absolute path corresponding to the 'path'
     *      argument of setCondition().
     *      Second component: the file contents given as the 'value' argument
     *      of setCondition().
     */
    std::pair<std::string, std::string> getCondition() const;

    /**
     * Set a predicate on all future operations. Future operations will return
     * Status::CONDITION_NOT_MET and have no effect unless the file at 'path'
     * has the contents 'value'. To remove the predicate, pass an empty string
     * as 'path'.
     * \param path
     *      The relative or absolute path to the file that must have the
     *      contents specified in value, or an empty string to clear the
     *      condition.
     * \param value
     *      The contents that the file specified by 'path' must have for future
     *      operations to succeed. If 'value' is the empty string and the
     *      file does not exist, the condition will also be satisfied.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *      If this returns an error, future operations on this tree will fail
     *      until a new condition is set or the condition is cleared.
     */
    Result setCondition(const std::string& path, const std::string& value);

    /**
     * Like setCondition but throws exceptions upon errors.
     */
    void setConditionEx(const std::string& path, const std::string& value);

    /**
     * Return the timeout set by a previous call to setTimeout().
     * \return
     *      The maximum duration of each operation (in nanoseconds), or 0
     *      for no timeout.
     */
    uint64_t getTimeout() const;

    /**
     * Abort each future operation if it may not have completed within the
     * specified period of time.
     * \warning
     *      The client library does not currently implement timeouts for DNS
     *      lookups. See https://github.com/logcabin/logcabin/issues/75
     * \param nanoseconds
     *      The maximum duration of each operation (in nanoseconds). Set to 0
     *      for no timeout.
     */
    void setTimeout(uint64_t nanoseconds);

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
     *       - CONDITION_NOT_MET if predicate from setCondition() was false.
     *       - TIMEOUT if timeout elapsed before the operation completed.
     */
    Result
    makeDirectory(const std::string& path);

    /**
     * Like makeDirectory but throws exceptions upon errors.
     */
    void makeDirectoryEx(const std::string& path);

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
     *       - CONDITION_NOT_MET if predicate from setCondition() was false.
     *       - TIMEOUT if timeout elapsed before the operation completed.
     */
    Result
    listDirectory(const std::string& path,
                  std::vector<std::string>& children) const;

    /**
     * Like listDirectory but throws exceptions upon errors.
     */
    std::vector<std::string> listDirectoryEx(const std::string& path) const;

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
     *       - CONDITION_NOT_MET if predicate from setCondition() was false.
     *       - TIMEOUT if timeout elapsed before the operation completed.
     */
    Result
    removeDirectory(const std::string& path);

    /**
     * Like removeDirectory but throws exceptions upon errors.
     */
    void
    removeDirectoryEx(const std::string& path);

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
     *       - CONDITION_NOT_MET if predicate from setCondition() was false.
     *       - TIMEOUT if timeout elapsed before the operation completed.
     */
    Result
    write(const std::string& path, const std::string& contents);

    /**
     * Like write but throws exceptions upon errors.
     */
    void
    writeEx(const std::string& path, const std::string& contents);

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
     *       - CONDITION_NOT_MET if predicate from setCondition() was false.
     *       - TIMEOUT if timeout elapsed before the operation completed.
     */
    Result
    read(const std::string& path, std::string& contents) const;

    /**
     * Like read but throws exceptions upon errors.
     */
    std::string
    readEx(const std::string& path) const;

    /**
     * Make sure a file does not exist.
     * \param path
     *      The path where there should not be a file after this call.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a directory.
     *       - CONDITION_NOT_MET if predicate from setCondition() was false.
     *       - TIMEOUT if timeout elapsed before the operation completed.
     */
    Result
    removeFile(const std::string& path);

    /**
     * Like removeFile but throws exceptions upon errors.
     */
    void
    removeFileEx(const std::string& path);

  private:
    /**
     * Get a reference to the implementation-specific members of this class.
     */
    std::shared_ptr<const TreeDetails> getTreeDetails() const;
    /**
     * Provides mutual exclusion to treeDetails pointer.
     */
    mutable std::mutex mutex;
    /**
     * Reference-counted pointer to implementation-specific members. This is
     * copy-on-write, so 'mutex' need not be held after taking a reference to
     * treeDetails.
     */
    std::shared_ptr<const TreeDetails> treeDetails;
    friend class Cluster;
};

/**
 * When running in testing mode, these callbacks serve as a way for the
 * application to interpose on requests and responses to inject failures and
 * model dynamic scenarios. See Cluster's constructor for more information.
 *
 * This is experimental and is not part of LogCabin's public API.
 */
class TestingCallbacks {
  public:

    /**
     * Constructor. Does nothing.
     */
    TestingCallbacks();

    /**
     * Destructor. Does nothing.
     */
    virtual ~TestingCallbacks();

    /**
     * Handle a read-only state machine query, such as Tree::read or
     * Tree::listDirectory.
     * The default implementation just returns false.
     * \param[in,out] request
     *      Protocol buffer containing request details. You can modify this and
     *      return false to have a slightly different request executed against
     *      the in-memory data structure.
     * \param[out] response
     *      Protocol buffer where response details should be filled in if true
     *      is returned.
     * \return
     *      True if handled, false to query the in-memory data structure.
     */
    virtual bool stateMachineQuery(
        Protocol::Client::StateMachineQuery_Request& request,
        Protocol::Client::StateMachineQuery_Response& response);

    /**
     * Handle a read-write state machine command, such as Tree::read or
     * Tree::listDirectory.
     * The default implementation just returns false.
     * \param[in,out] request
     *      Protocol buffer containing request details. You can modify this and
     *      return false to have a slightly different request executed against
     *      the in-memory data structure.
     * \param[out] response
     *      Protocol buffer where response details should be filled in if true
     *      is returned.
     * \return
     *      True if handled, false to query the in-memory data structure.
     */
    virtual bool stateMachineCommand(
        Protocol::Client::StateMachineCommand_Request& request,
        Protocol::Client::StateMachineCommand_Response& response);
};


/**
 * A handle to the LogCabin cluster.
 *
 * If the client requests changes to the cluster's replicated state machine
 * (for example, by writing a value), the client library will first open a
 * session with the cluster. It will thereafter periodically send keep-alive
 * requests to the cluster during periods of inactivity to maintain this
 * session. If communication to the LogCabin cluster is lost for an extended
 * period of time, the client's session will expire, and this library will
 * force the client to crash.
 */
class Cluster {
  public:

    /**
     * Settings for the client library. These are all optional.
     * Currently supported options:
     * - clusterUUID (see sample.conf)
     * - tcpHeartbeatTimeoutMilliseconds (see sample.conf)
     * - tcpConnectTimeoutMilliseconds (see sample.conf)
     * - sessionCloseTimeoutMilliseconds:
     *      This Cluster object opens a session with LogCabin before issuing
     *      any read-write commands to the replicated state machine. When this
     *      Cluster object is destroyed, it will attempt to close its session
     *      gracefully. This timeout controls the number of milliseconds that
     *      the client will wait until giving up on the close session RPC. It
     *      defaults to tcpConnectTimeoutMilliseconds, since they should be on
     *      the same order of magnitude.
     */
    typedef std::map<std::string, std::string> Options;

    /**
     * Construct a Cluster object for testing purposes only. Instead of
     * connecting to a LogCabin cluster, it will keep all state locally in
     * memory.
     *
     * This is experimental and is not part of LogCabin's public API.
     *
     * \param testingCallbacks
     *      These allow the application to interpose on requests. A
     *      default-constructed TestingCallbacks will execute requests against
     *      an in-memory structure. Applications can pass in classes derived
     *      from TestingCallbacks to model failures and more dynamic scenarios.
     * \param options
     *      Settings for the client library (see #Options).
     */
    explicit Cluster(std::shared_ptr<TestingCallbacks> testingCallbacks,
                     const Options& options = Options());

    /**
     * Constructor.
     * \param hosts
     *      A string describing the hosts in the cluster. This should be of the
     *      form host:port, where host is usually a DNS name that resolves to
     *      multiple IP addresses. Alternatively, you can pass a list of hosts
     *      as host1:port1,host2:port2,host3:port3.
     * \param options
     *      Settings for the client library (see #Options).
     */
    explicit Cluster(const std::string& hosts,
                     const Options& options = Options());

    /**
     * Destructor.
     */
    ~Cluster();

    /**
     * Get the current, stable cluster configuration.
     * \return
     *      first: configurationId: Identifies the configuration.
     *             Pass this to setConfiguration later.
     *      second: The list of servers in the configuration.
     */
    std::pair<uint64_t, Configuration> getConfiguration() const;

    /**
     * Change the cluster's configuration.
     * \param oldId
     *      The ID of the cluster's current configuration.
     * \param newConfiguration
     *      The list of servers in the new configuration.
     */
    ConfigurationResult setConfiguration(
                                uint64_t oldId,
                                const Configuration& newConfiguration);

    /**
     * Retrieve basic information from the given server, like its ID and the
     * addresses on which it is listening.
     * \param host
     *      The hostname or IP address of the server to retrieve stats from. It
     *      is recommended that you do not use a DNS name that resolves to
     *      multiple hosts here.
     * \param timeoutNanoseconds
     *      Abort the operation if it has not completed within the specified
     *      period of time. Time is specified in nanoseconds, and the special
     *      value of 0 indicates no timeout.
     * \warning
     *      The client library does not currently implement timeouts for DNS
     *      lookups. See https://github.com/logcabin/logcabin/issues/75
     * \param[out] info
     *      Protocol buffer of Stats as retrieved from the server.
     * \return
     *      Either OK or TIMEOUT.
     */
    Result
    getServerInfo(const std::string& host,
                  uint64_t timeoutNanoseconds,
                  Server& info);
    /**
     * Like getServerInfo but throws exceptions upon errors.
     */
    Server
    getServerInfoEx(const std::string& host,
                    uint64_t timeoutNanoseconds);

    /**
     * Retrieve statistics from the given server, which are useful for
     * diagnostics.
     * \param host
     *      The hostname or IP address of the server to retrieve stats from. It
     *      is recommended that you do not use a DNS name that resolves to
     *      multiple hosts here.
     * \param timeoutNanoseconds
     *      Abort the operation if it has not completed within the specified
     *      period of time. Time is specified in nanoseconds, and the special
     *      value of 0 indicates no timeout.
     * \warning
     *      The client library does not currently implement timeouts for DNS
     *      lookups. See https://github.com/logcabin/logcabin/issues/75
     * \param[out] stats
     *      Protocol buffer of Stats as retrieved from the server.
     * \return
     *      Either OK or TIMEOUT.
     */
    Result
    getServerStats(const std::string& host,
                   uint64_t timeoutNanoseconds,
                   Protocol::ServerStats& stats);
    /**
     * Like getServerStats but throws exceptions upon errors.
     */
    Protocol::ServerStats
    getServerStatsEx(const std::string& host,
                     uint64_t timeoutNanoseconds);

    /**
     * Return an object to access the hierarchical key-value store.
     * \return
     *      A Tree object with the working directory of '/'.
     */
    Tree getTree();

  private:
    std::shared_ptr<ClientImpl> clientImpl;
};

} // namespace LogCabin::Client
} // namespace LogCabin

#endif /* LOGCABIN_INCLUDE_LOGCABIN_CLIENT_H */
