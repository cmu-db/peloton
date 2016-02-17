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

#ifndef LOGCABIN_CORE_ROLLINGSTAT_H
#define LOGCABIN_CORE_ROLLINGSTAT_H

#include <cinttypes>
#include <iostream>
#include <deque>

#include "Core/Time.h"

namespace LogCabin {

// forward declaration
namespace Protocol {
class RollingStat;
}

namespace Core {

/**
 * This class gathers statistics about a given metric over time, like its
 * average, standard deviation, and exponentially weighted moving average. This
 * class also keeps track of the last 5 "exceptional" values, typically those
 * that are above some pre-defined threshold.
 *
 * This currently assumes your metric is a uint64_t. It could probably be
 * abstracted out in some way, but most metrics in LogCabin seem to fit this
 * category.
 */
class RollingStat {
  public:
    /**
     * Clock used for exceptional values.
     */
    typedef Core::Time::SteadyClock Clock;
    /**
     * Time point used for exceptional values.
     */
    typedef Clock::time_point TimePoint;

    /**
     * Constructor.
     */
    RollingStat();

    /**
     * Destructor.
     */
    ~RollingStat();

    /**
     * Return mean, or 0 if no values reported.
     */
    double getAverage() const;
    /**
     * Return number of values reported.
     */
    uint64_t getCount() const;
    /**
     * Return exponentially weighted moving average with alpha of 0.5,
     * or 0 if no values reported.
     */
    double getEWMA2() const;
    /**
     * Return exponentially weighted moving average with alpha of 0.25,
     * or 0 if no values reported.
     */
    double getEWMA4() const;
    /**
     * Return total number of exceptional values reported.
     */
    uint64_t getExceptionalCount() const;
    /**
     * Return last value reported, or 0 if no values reported.
     */
    uint64_t getLast() const;
    /**
     * Return up to the last 5 exceptional values reported, newest first.
     */
    std::vector<std::pair<TimePoint, uint64_t>> getLastExceptional() const;
    /**
     * Return the smallest value reported, or 0 if no values reported.
     */
    uint64_t getMin() const;
    /**
     * Return the largest value reported, or 0 if no values reported.
     */
    uint64_t getMax() const;
    /**
     * Return the cumulative total of all values reported, or 0 if no values
     * reported.
     */
    uint64_t getSum() const;
    /**
     * Return the standard deviation of all values reported, or 0 if no values
     * reported.
     */
    double getStdDev() const;

    /**
     * Report an exceptional value. Note that this does not include a 'push';
     * you may want to do that separately.
     * \param when
     *      The time the exceptional situation occurred (by convention, this is
     *      usually its start time).
     * \param value
     *      The exceptional value.
     */
    void noteExceptional(TimePoint when, uint64_t value);

    /**
     * Report a value.
     */
    void push(uint64_t value);

    /**
     * Serialize all the stats into the given empty ProtoBuf message.
     */
    void updateProtoBuf(Protocol::RollingStat& message) const;

    /**
     * Print all the stats.
     */
    friend std::ostream& operator<<(std::ostream& os,
                                    const RollingStat& stat);

  private:
    // See getters above for most of these.
    uint64_t count;
    double ewma2;
    double ewma4;
    uint64_t exceptionalCount;
    uint64_t last;
    std::deque<std::pair<TimePoint, uint64_t>> lastExceptional;
    uint64_t max;
    uint64_t min;
    uint64_t sum;
    /**
     * Used to calculate standard deviation. sum of x times x over all values.
     */
    uint64_t sumSquares;

};

} // namespace LogCabin::Core
} // namespace LogCabin

#endif /* LOGCABIN_CORE_ROLLINGSTAT_H */
