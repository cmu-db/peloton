/* Copyright (c) 2010-2012 Stanford University
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
 * This file is used to control how LogCabin's debug log (event log) messages
 * are handled. When used by client applications, this should be accessed as
 * LogCabin::Client::Debug (the LogCabin::Core namespace is used internally in
 * LogCabin).
 */

#include <cstdio>
#include <initializer_list>
#include <functional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#ifndef LOGCABIN_INCLUDE_LOGCABIN_DEBUG_H
#define LOGCABIN_INCLUDE_LOGCABIN_DEBUG_H

namespace LogCabin {
namespace Core {
namespace Debug {

/**
 * The levels of verbosity for log messages. Higher values are noisier.
 *
 * \since LogCabin v1.1.0. New levels may be added in future minor releases
 * (adding a new level is considered backwards-compatible), so use 'default'
 * values in switch statements.
 */
enum class LogLevel {
    // If you change this enum, you should also update logLevelToString() and
    // logLevelFromString() in Core/Debug.cc.
    /**
     * This log level is just used for disabling all log messages, which is
     * really only useful in unit tests.
     */
    SILENT = 0,
    /**
     * Bad stuff that shouldn't happen. The system broke its contract to users
     * in some way or some major assumption was violated.
     */
    ERROR = 10,
    /**
     * Messages at the WARNING level indicate that, although something went
     * wrong or something unexpected happened, it was transient and
     * recoverable.
     */
    WARNING = 20,
    /**
     * A system message that might be useful for administrators and developers.
     */
    NOTICE = 30,
    /**
     * Messages at the VERBOSE level don't necessarily indicate that anything
     * went wrong, but they could be useful in diagnosing problems.
     */
    VERBOSE = 40,
};

/**
 * When LogCabin wants to print a log message, this is the information that
 * gets included.
 */
struct DebugMessage {
    /// Default constructor.
    DebugMessage();
    /// Copy constructor.
    DebugMessage(const DebugMessage& other);
    /// Move constructor.
    DebugMessage(DebugMessage&& other);
    /// Destructor.
    ~DebugMessage();
    /// Copy assignment.
    DebugMessage& operator=(const DebugMessage& other);
    /// Move assignment.
    DebugMessage& operator=(DebugMessage&& other);

    /// The output of __FILE__.
    const char* filename;
    /// The output of __LINE__.
    int linenum;
    /// The output of __FUNCTION__.
    const char* function;
    /// The level of importance of the message as an integer. This should have
    /// been of type LogLevel but int was exposed originally; int is used for
    /// backwards compatibility.
    int logLevel;
    /// The level of importance of the message as a static string.
    const char* logLevelString;
    /// The name of the current process (its PID or server ID).
    std::string processName;
    /// The name of the current thread (by its function or its thread ID).
    std::string threadName;
    /// The contents of the message.
    std::string message;
};

/**
 * Return the filename given to the last successful call to setLogFilename(),
 * or the empty string if none.
 * \since LogCabin v1.1.0.
 */
std::string getLogFilename();

/**
 * Open the given file by name and append future debug log messages to it.
 * Note that if a handler is set with setLogHandler, this file will not be
 * used.
 * \param filename
 *      Name of file. If it already exists, new messages will be appended at
 *      the end. If the file is already open, this will re-open it (useful for
 *      rotating logs).
 * \return
 *      Error message if errors were encountered opening the file, otherwise an
 *      empty string indicates success.
 * \since LogCabin v1.1.0.
 */
std::string setLogFilename(const std::string& filename);

/**
 * Called to rotate the log file.
 * If there was a previous call to setLogFilename(), this will reopen that file
 * by name, returning any errors. Otherwise, it will do nothing.
 * \return
 *      Error message if errors were encountered in reopening the file,
 *      otherwise an empty string indicates success.
 * \since LogCabin v1.1.0.
 */
std::string reopenLogFromFilename();

/**
 * Change the file on which debug log messages are written.
 *
 * Note that if a handler is set with setLogHandler, this file will not be
 * used. If a filename has been set with setLogFilename(), this will clear it.
 *
 * \param newFile
 *      Handle to open file where log messages will be written.
 * \return
 *      Handle to previous log file (initialized to stderr on process start).
 */
FILE*
setLogFile(FILE* newFile);


/**
 * Accept log messages on the given callback instead of writing them to a file.
 * Call this again with an empty std::function() to clear it.
 * \param newHandler
 *      Callback invoked once per log message, possibly concurrently.
 * \return
 *      Previous callback (initialized to empty std::function on process start).
 */
std::function<void(DebugMessage)>
setLogHandler(std::function<void(DebugMessage)> newHandler);

/**
 * Return the current log policy (as set by a previous call to setLogPolicy).
 * Note that this may be empty, indicating that the default level of NOTICE is
 * in use.
 * \since LogCabin v1.1.0.
 */
std::vector<std::pair<std::string, std::string>>
getLogPolicy();

/**
 * Specify the log messages that should be displayed for each filename.
 * This first component is a pattern; the second is a log level.
 * A filename is matched against each pattern in order: if the filename starts
 * with or ends with the pattern, the corresponding log level defines the most
 * verbose messages that are to be displayed for the file. If a filename
 * matches no pattern, its log level will default to NOTICE.
 */
void
setLogPolicy(const std::vector<
                        std::pair<std::string, std::string>>& newPolicy);
/**
 * See setLogPolicy.
 */
void
setLogPolicy(const std::initializer_list<
                        std::pair<std::string, std::string>>& newPolicy);

/**
 * Build a log policy from its string representation.
 * \param in
 *      A string of the form "pattern@level,pattern@level,level".
 *      The pattern is separated from the level by an at symbol. Multiple rules
 *      are separated by comma. A rule with an empty pattern (match all) does
 *      not need an at symbol.
 * \return
 *      Logging policy based on string description.
 * \since LogCabin v1.1.0.
 */
std::vector<std::pair<std::string, std::string>>
logPolicyFromString(const std::string& in);

/**
 * Serialize a log policy into a string representation.
 * \param policy
 *      Logging policy in the format required by setLogPolicy.
 * \return
 *      String representation as accepted by logPolicyFromString.
 * \since LogCabin v1.1.0.
 */
std::string
logPolicyToString(const std::vector<
                            std::pair<std::string, std::string>>& policy);

} // namespace LogCabin::Core::Debug
} // namespace LogCabin::Core
} // namespace LogCabin

#endif /* LOGCABIN_INCLUDE_LOGCABIN_DEBUG_H */
