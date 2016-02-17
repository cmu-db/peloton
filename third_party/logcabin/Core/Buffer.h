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

#include <cinttypes>
#include <cstdlib>

#ifndef LOGCABIN_CORE_BUFFER_H
#define LOGCABIN_CORE_BUFFER_H

namespace LogCabin {
namespace Core {

/**
 * A container for opaque data. This makes it easy for code to operate on data
 * without having to explicitly pass around its length and whether and how to
 * free its memory.
 */
class Buffer {
  public:
    /**
     * A deleter is a function that can free the memory contained in the
     * Buffer. Examples include C's free() function for memory allocated with
     * malloc(), and deleteObjectFn() and deleteArrayFn() defined in this
     * class.
     * \param data
     *      The memory that should be reclaimed.
     */
    typedef void (*Deleter)(void* data);

    /**
     * A Deleter that uses C++'s delete keyword.
     * This should be used with data that was allocated with C++'s new keyword.
     * For new[] and delete[], see deleteArrayFn.
     * \param data
     *      The memory that should be reclaimed.
     * \tparam T
     *      The type of the object as it was allocated with new.
     */
    template<typename T>
    static void deleteObjectFn(void* data) {
        delete static_cast<T>(data);
    }

    /**
     * A Deleter that uses C++'s array delete[] keyword.
     * This should be used with data that was allocated with C++'s array new[]
     * keyword. For new and delete on individual objects, see deleteArray.
     * \param data
     *      The memory that should be reclaimed.
     * \tparam T
     *      The type of each element of the array as it was allocated with
     *      new[].
     */
    template<typename T>
    static void deleteArrayFn(void* data) {
        delete[] static_cast<T*>(data);
    }

    /**
     * Default constructor.
     */
    Buffer();

    /**
     * Construct a non-empty buffer. This is equivalent to using the default
     * constructor and then calling setData().
     * \param data
     *      A pointer to the first byte of data to be contained in the Buffer.
     * \param length
     *      The length in bytes of data.
     * \param deleter
     *      Describes how to release the memory for 'data' when this Buffer is
     *      destroyed or its data is replaced. NULL indicates that the caller
     *      owns the memory and guarantees its lifetime will extend beyond that
     *      of the Buffer.
     */
    Buffer(void* data, uint64_t length, Deleter deleter);

    /**
     * Move constructor.
     */
    Buffer(Buffer&& other);

    /**
     * Destructor. The memory for the existing data, if any, will be reclaimed
     * according to its deleter.
     */
    ~Buffer();

    /**
     * Move assignment.
     */
    Buffer& operator=(Buffer&& other);

    /**
     * Return a pointer to the first byte of data.
     */
    void* getData() { return data; }

    /**
     * Return a pointer to the first byte of data (const variant).
     */
    const void* getData() const { return data; }

    /**
     * Return the number of bytes that make up the data.
     */
    uint64_t getLength() const { return length; }

    /**
     * Replace the data contained in this Buffer.
     * The memory for the previously existing data, if any, will be reclaimed
     * according to the previous deleter.
     * \param data
     *      A pointer to the first byte of data to be contained in the Buffer.
     * \param length
     *      The length in bytes of data.
     * \param deleter
     *      Describes how to release the memory for 'data' when this Buffer is
     *      destroyed or its data is replaced. NULL indicates that the caller
     *      owns the memory and guarantees its lifetime will extend beyond that
     *      of the Buffer.
     */
    void setData(void* data, uint64_t length, Deleter deleter);

    /**
     * Empty the Buffer. The memory for the existing data, if any, will be
     * reclaimed according to its deleter.
     */
    void reset();

  private:
    /**
     * A pointer to the data or NULL if none has been set.
     */
    void* data;

    /**
     * The number of bytes that make up data or 0 if none has been set.
     */
    uint64_t length;

    /**
     * Describes how to release the memory for 'data' when this Buffer is
     * destroyed or its data is replaced.
     * This may be NULL, in which case it is not called (the memory is managed
     * externally).
     */
    Deleter deleter;

    // Buffer is non-copyable.
    Buffer(const Buffer&) = delete;
    Buffer& operator=(const Buffer&) = delete;

}; // class Buffer

} // namespace LogCabin::Core
} // namespace LogCabin

#endif /* LOGCABIN_CORE_BUFFER_H */
