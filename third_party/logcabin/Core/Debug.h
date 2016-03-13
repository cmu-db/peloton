/* Copyright (c) 2010-2012 Stanford University
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

#include <cinttypes>
#include <cstdlib>
#include <string>

#include "include/LogCabin/Debug.h"

#ifndef LOGCABIN_CORE_DEBUG_H
#define LOGCABIN_CORE_DEBUG_H

namespace LogCabin {
namespace Core {
namespace Debug {

// Configuring logging is exposed to clients as well as servers,
// so that stuff goes in a public header file: "include/LogCabin/Debug.h"

/**
 * Output a LogLevel to a stream. Having this improves gtest error messages.
 */
std::ostream& operator<<(std::ostream& ostream, LogLevel level);

/**
 * Return whether the current logging configuration includes messages of
 * the given level for the given filename.
 * This is normally called by LOG().
 * \warning
 *      fileName must be a string literal!
 * \param level
 *      The log level to query.
 * \param fileName
 *      This should be a string literal, probably __FILE__, since the result of
 *      this call will be cached based on the memory address pointed to by
 *      'fileName'.
 */
bool
isLogging(LogLevel level, const char* fileName);

/**
 * Unconditionally log the given message to stderr.
 * This is normally called by LOG().
 * \param level
 *      The level of importance of the message.
 * \param fileName
 *      The output of __FILE__.
 * \param lineNum
 *      The output of __LINE__.
 * \param functionName
 *      The output of __FUNCTION__.
 * \param format
 *      A printf-style format string for the message. It should include a line
 *      break at the end.
 * \param ...
 *      The arguments to the format string, as in printf.
 */
void
log(LogLevel level,
    const char* fileName, uint32_t lineNum, const char* functionName,
    const char* format, ...)
__attribute__((format(printf, 5, 6)));

/**
 * A short name to be used in log messages to identify this process.
 * This defaults to the UNIX process ID.
 */
extern std::string processName;

} // namespace LogCabin::Core::Debug
} // namespace LogCabin::Core
} // namespace LogCabin

/**
 * Unconditionally log the given message to stderr.
 * This is normally called by ERROR(), WARNING(), NOTICE(), or VERBOSE().
 * \param level
 *      The level of importance of the message.
 * \param format
 *      A printf-style format string for the message. It should not include a
 *      line break at the end, as LOG will add one.
 * \param ...
 *      The arguments to the format string, as in printf.
 */
#define LOG(level, format, ...) do { \
    if (::LogCabin::Core::Debug::isLogging(level, __FILE__)) { \
        ::LogCabin::Core::Debug::log(level, \
            __FILE__, __LINE__, __FUNCTION__, \
            format "\n", ##__VA_ARGS__); \
    } \
} while (0)

/**
 * Log an ERROR message and abort the process.
 * \copydetails ERROR
 */
#define PANIC(format, ...) do { \
    ERROR(format " Exiting...", ##__VA_ARGS__); \
    ::abort(); \
} while (0)

/**
 * Log an ERROR message and exit the process with status 1.
 * \copydetails ERROR
 */
#define EXIT(format, ...) do { \
    ERROR(format " Exiting...", ##__VA_ARGS__); \
    ::exit(1); \
} while (0)

/**
 * Log an ERROR message.
 * \param format
 *      A printf-style format string for the message. It should not include a
 *      line break at the end, as LOG will add one.
 * \param ...
 *      The arguments to the format string, as in printf.
 */
#define ERROR(format, ...) \
    LOG((::LogCabin::Core::Debug::LogLevel::ERROR), format, ##__VA_ARGS__)

/**
 * Log a WARNING message.
 * \copydetails ERROR
 */
#define WARNING(format, ...) \
    LOG((::LogCabin::Core::Debug::LogLevel::WARNING), format, ##__VA_ARGS__)

/**
 * Log a NOTICE message.
 * \copydetails ERROR
 */
#define NOTICE(format, ...) \
    LOG((::LogCabin::Core::Debug::LogLevel::NOTICE), format, ##__VA_ARGS__)

/**
 * Log a VERBOSE message.
 * \copydetails ERROR
 */
#define VERBOSE(format, ...) \
    LOG((::LogCabin::Core::Debug::LogLevel::VERBOSE), format, ##__VA_ARGS__)

#endif /* LOGCABIN_CORE_DEBUG_H */
