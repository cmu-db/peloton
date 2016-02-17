/* Copyright (c) 2012 Stanford University
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

#include "Core/Buffer.h"

namespace LogCabin {
namespace Core {

Buffer::Buffer()
    : data(NULL)
    , length(0)
    , deleter(NULL)
{
}

Buffer::Buffer(void* data, uint64_t length, Deleter deleter)
    : data(data)
    , length(length)
    , deleter(deleter)
{
}

Buffer::Buffer(Buffer&& other)
    : data(other.data)
    , length(other.length)
    , deleter(other.deleter)
{
    other.data = NULL; // Not strictly necessary, but may help
    other.length = 0;  // catch bugs in caller code.
    other.deleter = NULL;
}

Buffer::~Buffer()
{
    if (deleter != NULL)
        (*deleter)(data);
}

Buffer&
Buffer::operator=(Buffer&& other)
{
    if (deleter != NULL)
        (*deleter)(data);
    data = other.data;
    length = other.length;
    deleter = other.deleter;
    other.data = NULL; // Not strictly necessary, but may help
    other.length = 0;  // catch bugs in caller code.
    other.deleter = NULL;
    return *this;
}

void
Buffer::setData(void* data, uint64_t length, Deleter deleter)
{
    if (this->deleter != NULL)
        (*this->deleter)(this->data);
    this->data = data;
    this->length = length;
    this->deleter = deleter;
}

void
Buffer::reset()
{
    if (deleter != NULL)
        (*deleter)(data);
    data = NULL;
    length = 0;
    deleter = NULL;
}

} // namespace LogCabin::Core
} // namespace LogCabin
