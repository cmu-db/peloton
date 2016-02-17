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

#include <cmath>

#include "build/Protocol/ServerStats.pb.h"
#include "Core/RollingStat.h"
#include "Core/StringUtil.h"

namespace LogCabin {
namespace Core {

RollingStat::RollingStat()
    : count(0)
    , ewma2(0)
    , ewma4(0)
    , exceptionalCount(0)
    , last(0)
    , lastExceptional()
    , max(0)
    , min(0)
    , sum(0)
    , sumSquares(0)
{
}

RollingStat::~RollingStat()
{
}

double
RollingStat::getAverage() const
{
    if (count == 0)
        return 0.0;
    return double(sum) / double(count);
}

uint64_t
RollingStat::getCount() const
{
    return count;
}

double
RollingStat::getEWMA2() const
{
    return ewma2;
}

double
RollingStat::getEWMA4() const
{
    return ewma4;
}

uint64_t
RollingStat::getExceptionalCount() const
{
    return exceptionalCount;
}

std::vector<std::pair<RollingStat::TimePoint, uint64_t>>
RollingStat::getLastExceptional() const
{
    std::vector<std::pair<RollingStat::TimePoint, uint64_t>> ret;
    ret.insert(ret.begin(), lastExceptional.begin(), lastExceptional.end());
    return ret;
}

uint64_t
RollingStat::getLast() const
{
    return last;
}

uint64_t
RollingStat::getMin() const
{
    return min;
}

uint64_t
RollingStat::getMax() const
{
    return max;
}

uint64_t
RollingStat::getSum() const
{
    return sum;
}

double
RollingStat::getStdDev() const
{
    if (count == 0)
        return 0.0;
    return (sqrt(double(count) * double(sumSquares) - double(sum * sum)) /
            double(count));
}

void
RollingStat::noteExceptional(TimePoint when, uint64_t value)
{
    ++exceptionalCount;
    if (lastExceptional.size() == 5)
        lastExceptional.pop_back();
    lastExceptional.emplace_front(when, value);
}

void
RollingStat::push(uint64_t value)
{
    ++count;

    if (count == 1)
        ewma2 = double(value);
    else
        ewma2 = 0.5 * double(value) + 0.5 * ewma2;

    if (count == 1)
        ewma4 = double(value);
    else
        ewma4 = 0.25 * double(value) + 0.75 * ewma4;

    last = value;

    if (value > max)
        max = value;

    if (value < min || count == 1)
        min = value;

    sum += value;

    sumSquares += value * value;
}

void
RollingStat::updateProtoBuf(Protocol::RollingStat& message) const
{
    message.set_count(getCount());
    if (getCount() > 0) {
        message.set_average(getAverage());
        message.set_ewma2(getEWMA2());
        message.set_ewma4(getEWMA4());
        message.set_last(getLast());
        message.set_min(getMin());
        message.set_max(getMax());
        message.set_sum(getSum());
        message.set_stddev(getStdDev());
    }
    message.set_exceptional_count(getExceptionalCount());
    Core::Time::SteadyTimeConverter timeConverter;
    for (auto it = lastExceptional.begin();
         it != lastExceptional.end();
         ++it) {
        Protocol::RollingStat::Exceptional& e =
            *message.add_last_exceptional();
        e.set_when(timeConverter.unixNanos(it->first));
        e.set_value(it->second);
    }
}

std::ostream&
operator<<(std::ostream& os, const RollingStat& stat)
{
    os << "count: " << stat.getCount() << std::endl;
    if (stat.getCount() > 0) {
        os << "average: " << stat.getAverage() << std::endl;
        os << "EWMA-2: " << stat.getEWMA2() << std::endl;
        os << "EWMA-4: " << stat.getEWMA4() << std::endl;
        os << "last: " << stat.getLast() << std::endl;
        os << "min: " << stat.getMin() << std::endl;
        os << "max: " << stat.getMax() << std::endl;
        os << "sum: " << stat.getSum() << std::endl;
        os << "stddev: " << stat.getStdDev() << std::endl;
    }
    os << "exceptional: " << stat.getExceptionalCount() << std::endl;
    if (!stat.lastExceptional.empty()) {
        os << "Last " << stat.lastExceptional.size() << " exceptional values:"
           << std::endl;
        Core::Time::SteadyTimeConverter timeConverter;
        for (auto it = stat.lastExceptional.begin();
             it != stat.lastExceptional.end();
             ++it) {
            int64_t nanos = timeConverter.unixNanos(it->first);
            os << it->second
               << " at "
               << Core::StringUtil::format("%ld.%09ld",
                                           nanos / 1000000000U,
                                           nanos % 1000000000U)
               << std::endl;
        }
    }
    return os;
}

} // namespace LogCabin::Core
} // namespace LogCabin
