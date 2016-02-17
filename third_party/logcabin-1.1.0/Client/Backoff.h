/* Copyright (c) 2014-2015 Diego Ongaro
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

#ifndef LOGCABIN_CLIENT_BACKOFF_H
#define LOGCABIN_CLIENT_BACKOFF_H

#include <cinttypes>
#include <deque>
#include <mutex>

#include "Core/Time.h"

namespace LogCabin {
namespace Client {

/**
 * A simple backoff mechanism. Currently used in the client library to
 * rate-limit the creation of new TCP connections.
 */
class Backoff {
  public:
    /// Clock used for keeping track of when operations started.
    typedef Core::Time::SteadyClock Clock;
    /// Point in time on #Clock. Used for timeouts.
    typedef Clock::time_point TimePoint;

    /**
     * Constructor.
     * \param windowCount
     *      At most this many operations are allowed in any windowNanos period
     *      of time.
     * \param windowNanos
     *      The duration over which at most windowCount operations are allowed.
     * \warning
     *      The memory usage of this class is proportional to windowCount.
     */
    Backoff(uint64_t windowCount, uint64_t windowNanos);

    /**
     * Destructor.
     */
    ~Backoff();

    /**
     * This is invoked before beginning a new operation. If the operation may
     * not proceed yet, this method sleeps until starting the operation becomes
     * permissible.
     * \param timeout
     *      Maximum time at which to stop sleeping and return, without marking
     *      the operation as having started.
     */
    void delayAndBegin(TimePoint timeout);

  private:

    /**
     * Protects all of the following member variables in this class.
     */
    std::mutex mutex;

    /**
     * At most #windowCount operations are allowed in any #windowDuration
     * period of time.
     */
    uint64_t windowCount;

    /**
     * See #windowCount.
     */
    std::chrono::nanoseconds windowDuration;

    /**
     * The times when the last #windowCount operations were initiated. If fewer
     * than #windowCount operations have been initiated, this is padded with
     * negative infinity. The front time is the oldest, and the back is the
     * most recent.
     */
    std::deque<TimePoint> startTimes;
};

} // namespace LogCabin::Client
} // namespace LogCabin

#endif /* LOGCABIN_CLIENT_BACKOFF_H */
