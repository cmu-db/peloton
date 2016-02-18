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

/**
 * \file
 * This file declares helper functions that are useful in LogCabin clients.
 */

#include <cstddef>
#include <memory>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#ifndef LOGCABIN_INCLUDE_LOGCABIN_UTIL_H
#define LOGCABIN_INCLUDE_LOGCABIN_UTIL_H

namespace LogCabin {
namespace Client {
namespace Util {

/**
 * Convert a human-readable description of a time duration into a number of
 * nanoseconds.
 * \param description
 *      Something like 10, 10s, -200ms, 3us, or -999ns. With no units, defaults
 *      to seconds. May be negative.
 *      Allowed units:
 *          ns, nanosecond(s),
 *          ms, millisecond(s),
 *          s, second(s),
 *          min, minute(s),
 *          h, hr, hour(s),
 *          d, day(s),
 *          w, wk, week(s),
 *          mo, month(s),
 *          y, yr, year(s).
 * \return
 *      Number of nanoseconds (may be negative, capped to the range of a signed
 *      64-bit integer).
 * \throw Client::InvalidArgumentException
 *      If description could not be parsed successfully.
 * \warning
 *      This function is subject to change. It is not subject to the versioning
 *      requirements of LogCabin's public API.
 */
int64_t parseSignedDuration(const std::string& description);

/**
 * Convert a human-readable description of a time duration into a number of
 * nanoseconds.
 * \param description
 *      Something like 10, 10s, 200ms, 3us, or 999ns. With no units, defaults
 *      to seconds. May not be negative.
 *      Allowed units:
 *          ns, nanosecond(s),
 *          ms, millisecond(s),
 *          s, second(s),
 *          min, minute(s),
 *          h, hr, hour(s),
 *          d, day(s),
 *          w, wk, week(s),
 *          mo, month(s),
 *          y, yr, year(s).
 * \return
 *      Number of nanoseconds (will not be negative, capped to the range of a
 *      signed 64-bit integer on the high end).
 * \throw Client::InvalidArgumentException
 *      If description could not be parsed successfully.
 * \warning
 *      This function is subject to change. It is not subject to the versioning
 *      requirements of LogCabin's public API.
 */
uint64_t parseNonNegativeDuration(const std::string& description);


} // namespace LogCabin::Util
} // namespace LogCabin::Client
} // namespace LogCabin

#endif /* LOGCABIN_INCLUDE_LOGCABIN_UTIL_H */
