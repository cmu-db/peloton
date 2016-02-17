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

#include <sstream>
#include <string>
#include <vector>

#ifndef LOGCABIN_CORE_STRINGUTIL_H
#define LOGCABIN_CORE_STRINGUTIL_H

namespace LogCabin {
namespace Core {
namespace StringUtil {

/**
 * A safe version of sprintf.
 */
std::string format(const char* format, ...)
    __attribute__((format(printf, 1, 2)));

/**
 * Format an ORed group of flags as a string.
 * \param value
 *      The ORed options.
 * \param flags
 *      Maps option identifiers to their string names, such as
 *      {{FOO, "FOO"}, {BAR, "BAR"}}.
 * \return
 *      String such as "FOO|BAR".
 */
std::string
flags(int value,
      std::initializer_list<std::pair<int, const char*>> flags);

/**
 * Determine whether a null-terminated string is printable.
 * \param str
 *      A null-terminated string.
 * \return
 *      True if all the bytes of str before its null terminator are nice to
 *      display in a single line of text.
 */
bool
isPrintable(const char* str);

/**
 * Determine whether some data is a printable, null-terminated string.
 * \param data
 *      The first byte.
 * \param length
 *      The number of bytes of 'data'.
 * \return
 *      True if the last byte of data is a null terminator and all the bytes of
 *      data before that are nice to display in a single line of text.
 */
bool
isPrintable(const void* data, size_t length);

std::string
join(const std::vector<std::string>& components, const std::string& glue);

/**
 * For strings, replace all occurrences of 'needle' in 'haystack' with
 * 'replacement'.
 *
 * If this isn't what you're looking for, the standard algorithm std::replace
 * might help you.
 */
void
replaceAll(std::string& haystack,
           const std::string& needle,
           const std::string& replacement);

/**
 * Split a string into multiple components by a character.
 * \param subject
 *      The string to split.
 * \param delimiter
 *      The character to split the string by.
 * \return
 *      The components of 'subject', not including 'delimiter'.
 *      - If two delimiters occur in a row in 'subject', a corresponding empty
 *        string will appear in the returned vector.
 *      - If a delimiter occurs at the start of 'subject', a corresponding
 *        empty string will appear at the start of the returned vector.
 *      - If a delimiter occurs at the end of 'subject', no corresponding empty
 *        string will appear at the end of the returned vector.
 */
std::vector<std::string>
split(const std::string& subject, char delimiter);

/// Return true if haystack begins with needle.
bool startsWith(const std::string& haystack, const std::string& needle);

/// Return true if haystack ends with needle.
bool endsWith(const std::string& haystack, const std::string& needle);

/**
 * Return a string returned from the given object's stream operator.
 * This is useful when you're dealing with strings, but the object you want to
 * print only has a stream operator.
 */
template<typename T>
std::string
toString(const T& t)
{
    std::stringstream ss;
    ss << t;
    return ss.str();
}

/**
 * Return a copy of the given string except with no leading or trailing
 * whitespace.
 */
std::string trim(const std::string& s);

} // namespace LogCabin::Core::StringUtil
} // namespace LogCabin::Core
} // namespace LogCabin

#endif /* LOGCABIN_CORE_STRINGUTIL_H */
