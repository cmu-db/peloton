/* Copyright (c) 2012 Stanford University
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
 */

#include <cinttypes>

#ifndef LOGCABIN_CORE_RANDOM_H
#define LOGCABIN_CORE_RANDOM_H

namespace LogCabin {
namespace Core {

/**
 * Thread-safe pseudo-random number generator.
 * \warning
 *      Don't call these functions from static initializers or you might run
 *      into the static initialization order fiasco.
 */
namespace Random {

/// Return one random byte.
uint8_t random8();
/// Return two random bytes.
uint16_t random16();
/// Return four random bytes.
uint32_t random32();
/// Return eight random bytes.
uint64_t random64();
/// Return a random floating point number between start and end, inclusive.
double randomRangeDouble(double start, double end);
/// Return a random integer between start and end, inclusive.
uint64_t randomRange(uint64_t start, uint64_t end);


} // namespace LogCabin::Core::Random

} // namespace LogCabin::Core
} // namespace LogCabin

#endif /* LOGCABIN_CORE_RANDOM_H */
