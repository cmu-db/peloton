/** \file */

#ifndef _CUCKOOHASH_UTIL_HH
#define _CUCKOOHASH_UTIL_HH

#include <exception>
#include <pthread.h>
#include "cuckoohash_config.hh" // for LIBCUCKOO_DEBUG

#if LIBCUCKOO_DEBUG
#  define LIBCUCKOO_DBG(fmt, args...)                                   \
     fprintf(stderr, "\x1b[32m""[libcuckoo:%s:%d:%lu] " fmt"" "\x1b[0m", \
             __FILE__,__LINE__, (unsigned long)pthread_self(), ##args)
#else
#  define LIBCUCKOO_DBG(fmt, args...)  do {} while (0)
#endif

// For enabling certain methods based on a condition. Here's an example.
// ENABLE_IF(some_cond, type, static, inline) method() {
//     ...
// }
#define ENABLE_IF(preamble, condition, return_type)                     \
    template <class Bogus=void*>                                        \
    preamble typename std::enable_if<sizeof(Bogus) &&                   \
        condition, return_type>::type

/**
 * Thrown when an automatic expansion is triggered, but the load factor of the
 * table is below a minimum threshold, which can be set by the \ref
 * cuckoohash_map::minimum_load_factor method. This can happen if the hash
 * function does not properly distribute keys, or for certain adversarial
 * workloads.
 */
class libcuckoo_load_factor_too_low : public std::exception {
public:
    /**
     * Constructor
     *
     * @param lf the load factor of the table when the exception was thrown
     */
    libcuckoo_load_factor_too_low(const double lf)
        : load_factor_(lf) {}

    virtual const char* what() const noexcept {
        return "Automatic expansion triggered when load factor was below "
            "minimum threshold";
    }

    /**
     * @return the load factor of the table when the exception was thrown
     */
    double load_factor() {
        return load_factor_;
    }
private:
    const double load_factor_;
};

/**
 * Thrown when an expansion is triggered, but the hashpower specified is greater
 * than the maximum, which can be set with the \ref
 * cuckoohash_map::maximum_hashpower method.
 */
class libcuckoo_maximum_hashpower_exceeded : public std::exception {
public:
    /**
     * Constructor
     *
     * @param hp the hash power we were trying to expand to
     */
    libcuckoo_maximum_hashpower_exceeded(const size_t hp)
        : hashpower_(hp) {}

    virtual const char* what() const noexcept {
        return "Expansion beyond maximum hashpower";
    }

    /**
     * @return the hashpower we were trying to expand to
     */
    size_t hashpower() {
        return hashpower_;
    }
private:
    const size_t hashpower_;
};

#endif // _CUCKOOHASH_UTIL_HH
