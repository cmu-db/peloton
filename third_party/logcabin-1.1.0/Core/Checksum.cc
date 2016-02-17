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

#include <map>
#include <memory>
#include <mutex>
#include <cstring>

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/cryptlib.h>
#include <cryptopp/crc.h>
#include <cryptopp/adler32.h>
#include <cryptopp/md5.h>
#include <cryptopp/sha.h>
#include <cryptopp/whrlpool.h>
#include <cryptopp/tiger.h>
#include <cryptopp/ripemd.h>

#include "Core/Debug.h"
#include "Core/Checksum.h"
#include "Core/STLUtil.h"
#include "Core/StringUtil.h"
#include "Core/Util.h"

namespace LogCabin {
namespace Core {
namespace Checksum {

using Core::Util::downCast;
using Core::StringUtil::format;

namespace {

/**
 * Helper for writeChecksum template, to keep code bloat to a minimum.
 */
uint32_t
writeChecksumHelper(
        CryptoPP::HashTransformation& hashFn,
        const char* name,
        std::initializer_list<std::pair<const void*, uint64_t>> data,
        char result[MAX_LENGTH])
{
    // Length of name in bytes, not including null character.
    const uint32_t nameLength = downCast<uint32_t>(strlen(name));
    // Size in bytes of binary hash function output.
    const uint32_t digestSize = hashFn.DigestSize();
    // Size in bytes of name:hexdigest string, including null character.
    const uint32_t outputSize = (nameLength + 1 +
                                 digestSize * 2 + 1);
    assert(outputSize <= MAX_LENGTH);

    // calculate binary digest
    uint8_t binary[digestSize];
    for (auto it = data.begin(); it != data.end(); ++it) {
        hashFn.Update(static_cast<const uint8_t*>(it->first),
                      it->second);
    }
    hashFn.Final(binary);

    // copy name and : to result
    memcpy(result, name, nameLength);
    result += nameLength;
    *result = ':';
    ++result;

    // add hex digest to result
    const char* hexArray = "0123456789abcdef";
    for (uint32_t i = 0; i < digestSize; ++i) {
        *result = hexArray[binary[i] >> 4];
        ++result;
        *result = hexArray[binary[i] & 15];
        ++result;
    }

    // add null terminator to result and return total length
    *result = '\0';
    return outputSize;
}

/**
 * Template to produce functions of type Algorithm when instantiated with a
 * CryptoPP::HashTransformation.
 */
template<typename HashFn>
uint32_t
writeChecksum(std::initializer_list<std::pair<const void*, uint64_t>> data,
              char result[MAX_LENGTH])
{
    HashFn hashFn;
    return writeChecksumHelper(hashFn,
                               HashFn::StaticAlgorithmName(),
                               data,
                               result);
}

/**
 * Type for function that calculate the checksum for some data.
 * \param data
 *      An list of (pointer, length) pairs describing what to checksum.
 * \param[out] result
 *      The result of the hash function will be placed here.
 *      This will be a null-terminated, printable C-string.
 * \return
 *      The number of valid characters in 'output', including the null
 *      terminator. This is guaranteed to be greater than 1.
 */
typedef uint32_t (*Algorithm)(
            std::initializer_list<std::pair<const void*, uint64_t>> data,
            char result[MAX_LENGTH]);

/**
 * A container for a set of Algorithm implementations.
 */
class Algorithms {
    /**
     * Helper for constructor().
     */
    template<typename ConcreteHashFunction>
    void registerAlgorithm()
    {
        // Using explicit types here to keep clang++-3.4 with libc++ happy.
        byName.insert(std::pair<std::string, Algorithm>(
                std::string(ConcreteHashFunction::StaticAlgorithmName()),
                Algorithm(writeChecksum<ConcreteHashFunction>)));
    }


  public:
    Algorithms()
        : byName()
    {
        registerAlgorithm<CryptoPP::CRC32>();
        registerAlgorithm<CryptoPP::Adler32>();
        registerAlgorithm<CryptoPP::Weak::MD5>();
        registerAlgorithm<CryptoPP::SHA1>();
        registerAlgorithm<CryptoPP::SHA224>();
        registerAlgorithm<CryptoPP::SHA256>();
        registerAlgorithm<CryptoPP::SHA384>();
        registerAlgorithm<CryptoPP::SHA512>();
        registerAlgorithm<CryptoPP::Whirlpool>();
        registerAlgorithm<CryptoPP::Tiger>();
        registerAlgorithm<CryptoPP::RIPEMD160>();
        registerAlgorithm<CryptoPP::RIPEMD320>();
        registerAlgorithm<CryptoPP::RIPEMD128>();
        registerAlgorithm<CryptoPP::RIPEMD256>();
    }

    /**
     * Find an algorithm by name.
     * \param name
     *      The short name of the algorithm. Must be null-terminated.
     * \return
     *      The Algorithm if found, NULL otherwise.
     */
    Algorithm find(const char* name)
    {
        // TODO(ongaro): This'll probably create a temporary std::string, which
        // might cause a memory allocation. But it's probably nothing to worry
        // about, and newer stdlib will hopefully implement a C++11-compliant
        // std::string type that contains a few characters without an
        // allocation.
        auto it = byName.find(name);
        if (it == byName.end())
            return NULL;
        else
            return it->second;
    }

    std::map<std::string, Algorithm> byName;
} algorithms;

} // namespace LogCabin::Core::Checksum::<anonymous>

std::vector<std::string>
listAlgorithms()
{
    return Core::STLUtil::getKeys(algorithms.byName);
}

uint32_t
calculate(const char* algorithm,
          const void* data, uint64_t dataLength,
          char output[MAX_LENGTH])
{
    return calculate(algorithm, {{data, dataLength}}, output);
}

uint32_t
calculate(const char* algorithm,
          std::initializer_list<std::pair<const void*, uint64_t>> data,
          char output[MAX_LENGTH])
{
    Algorithm algo = algorithms.find(algorithm);
    if (algo == NULL) {
        PANIC("The hashing algorithm %s is not available",
              algorithm);
    }
    return (*algo)(data, output);
}


uint32_t
length(const char* checksum,
       uint32_t maxChecksumLength)
{
    uint32_t len = 0;
    while (true) {
        if (len == maxChecksumLength || len == MAX_LENGTH)
            return 0;
        char c = checksum[len];
        ++len;
        if (c == '\0')
            return len;
    }
}

std::string
verify(const char* checksum,
       const void* data, uint64_t dataLength)
{
    return verify(checksum, {{data, dataLength}});
}

std::string
verify(const char* checksum,
       std::initializer_list<std::pair<const void*, uint64_t>> data)
{
    if (!Core::StringUtil::isPrintable(checksum))
        return "The given checksum value is corrupt and not printable.";

    Algorithm algo = NULL;
    { // find algo
        char algorithmName[MAX_LENGTH];
        char* colon = strchr(const_cast<char*>(checksum), ':');
        if (colon == NULL)
            return format("Missing colon in checksum: %s", checksum);

        // nameLength does not include the null terminator
        uint32_t nameLength = downCast<uint32_t>(colon - checksum);
        memcpy(algorithmName, checksum, nameLength);
        algorithmName[nameLength] = '\0';

         algo = algorithms.find(algorithmName);
         if (algo == NULL)
             return format("No such checksum algorithm: %s",
                                 algorithmName);
    }

    // compare calculated checksum with the one given
    char calculated[MAX_LENGTH];
    (*algo)(data, calculated);
    if (strcmp(calculated, checksum) != 0) {
        return format("Checksum doesn't match: expected %s "
                            "but calculated %s", checksum, calculated);
    }

    return std::string();
}

} // namespace LogCabin::Core::Checksum
} // namespace LogCabin::Core
} // namespace LogCabin
