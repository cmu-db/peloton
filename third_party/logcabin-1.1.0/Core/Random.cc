/* Copyright (c) 2009-2014 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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
 *
 *
 * Some of this code is copied from RAMCloud src/Common.cc _generateRandom(),
 * Copyright (c) 2009-2014 Stanford University also under the same ISC license.
 */

#include <cassert>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <mutex>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Core/Debug.h"
#include "Core/Random.h"

namespace LogCabin {
namespace Core {
namespace Random {

namespace {

// forward declaration
void resetRandomState();

/**
 * Keeps state needed by the random number generator, protected by a mutex.
 */
class RandomState {
  public:
    RandomState()
        : mutex()
        , init(false)
        , statebuf()
        , randbuf()
    {
        reset();
        int err = pthread_atfork(NULL, NULL, resetRandomState);
        if (err != 0) {
            // too early to call ERROR in here
            fprintf(stderr, "Failed to set up pthread_atfork() handler to "
                    "reset random number generator seed in child processes. "
                    "As a result, child processes will generate the same "
                    "sequence of random values as the parent they were forked "
                    "from. Error: %s\n",
                    strerror(err));
        }
    }

    void reset() {
        std::lock_guard<std::mutex> lockGuard(mutex);
        int fd = open("/dev/urandom", O_RDONLY);
        if (fd < 0) {
            // too early to call PANIC in here
            fprintf(stderr, "Couldn't open /dev/urandom: %s\n",
                    strerror(errno));
            abort();
        }
        unsigned int seed;
        ssize_t bytesRead = read(fd, &seed, sizeof(seed));
        close(fd);
        if (bytesRead != sizeof(seed)) {
            // too early to call PANIC in here
            fprintf(stderr, "Couldn't read full seed from /dev/urandom\n");
            abort();
        }
        initstate_r(seed, statebuf, STATE_BYTES, &randbuf);
        static_assert(RAND_MAX >= (1L << 30), "RAND_MAX too small");
        init = true;
    }

    uint64_t random64() {
        std::lock_guard<std::mutex> lockGuard(mutex);
        if (!init) {
            // probably too early to call PANIC in here
            fprintf(stderr, "Looks like you hit the so-called static "
                    "initialization order fiasco in Core::Random\n");
            abort();
        }

        // Each call to random returns 31 bits of randomness,
        // so we need three to get 64 bits of randomness.
        int32_t lo, mid, hi;
        random_r(&randbuf, &lo);
        random_r(&randbuf, &mid);
        random_r(&randbuf, &hi);
        uint64_t r = (((uint64_t(hi) & 0x7FFFFFFF) << 33) | // NOLINT
                      ((uint64_t(mid) & 0x7FFFFFFF) << 2)  | // NOLINT
                      (uint64_t(lo) & 0x00000003)); // NOLINT
        return r;
    }

  private:

    /**
     * Protect following members from concurrent access.
     */
    std::mutex mutex;

    /**
     * Set to true when the constructor completes.
     */
    bool init;

    /**
     * Size of 'statebuf'. 128 is the same size as initstate() uses for regular
     * random(), see manpages for details.
     */
    enum { STATE_BYTES = 128 };

    /**
     * Internal scratch state used by random_r.
     */
    char statebuf[STATE_BYTES];

    /**
     * random_r's state. Must be handed to each call, and seems to refer to
     * statebuf in some undocumented way.
     */
    random_data randbuf;
} randomState;

/**
 * Called in child after fork() to reset random seed.
 */
void
resetRandomState()
{
    randomState.reset();
}

/**
 * Fill a variable of type T with some random bytes.
 */
template<typename T>
T
getRandomBytes()
{
    T buf {};
    size_t offset = 0;
    while (offset < sizeof(buf)) {
        uint64_t r = randomState.random64();
        size_t copy = std::min(sizeof(r), sizeof(buf) - offset);
        memcpy(reinterpret_cast<char*>(&buf) + offset, &r, copy);
        offset += copy;
    }
    return buf;
}

/// Return a random number between 0 and 1.
double
randomUnit()
{
    return (double(random64()) /
            double(std::numeric_limits<uint64_t>::max()));
}

} // anonymous namespace

uint8_t
random8()
{
    return getRandomBytes<uint8_t>();
}

uint16_t
random16()
{
    return getRandomBytes<uint16_t>();
}

uint32_t
random32()
{
    return getRandomBytes<uint32_t>();
}

uint64_t
random64()
{
    return randomState.random64();
}

double
randomRangeDouble(double start, double end)
{
    return start + randomUnit()  * (end - start);
}

uint64_t
randomRange(uint64_t start, uint64_t end)
{
    return uint64_t(lround(randomRangeDouble(double(start), double(end))));
}

} // namespace LogCabin::Core::Random
} // namespace LogCabin::Core
} // namespace LogCabin
