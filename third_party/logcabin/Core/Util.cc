/* Copyright (c) 2011-2012 Stanford University
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

#include <cstring>
#include "Core/Util.h"

namespace LogCabin {
namespace Core {
namespace Util {

bool
isPowerOfTwo(uint64_t x)
{
    if (x == 0)
        return false;
    else
        return (x & (x - 1)) == 0;
}

void*
memcpy(void* dest,
       std::initializer_list<std::pair<const void*, size_t>> src)
{
    uint8_t* target = static_cast<uint8_t*>(dest);
    for (auto it = src.begin(); it != src.end(); ++it) {
        ::memcpy(target, it->first, it->second);
        target += it->second;
    }
    return dest;
}

ThreadInterruptedException::ThreadInterruptedException()
    : std::runtime_error("Thread was interrupted")
{
}

} // namespace LogCabin::Core::Util
} // namespace LogCabin::Core
} // namespace LogCabin
