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

#include <cassert>
#include <functional>
#include <mutex>

#include "Core/Debug.h"

#ifndef LOGCABIN_CORE_MUTEX_H
#define LOGCABIN_CORE_MUTEX_H

namespace LogCabin {
namespace Core {

/**
 * A wrapper around std::mutex that is useful for testing purposes. You can set
 * a callback to be called when the mutex is locked and before it is unlocked.
 * This callback can, for example, check the invariants on the protected state.
 *
 * The interface to this class is the same as std::mutex.
 */
class Mutex {
  public:
    typedef std::mutex::native_handle_type native_handle_type;

    Mutex()
        : m()
        , callback()
    {
    }

    void
    lock() {
        m.lock();
        if (callback)
            callback();
    }

    bool
    try_lock() {
        bool l = m.try_lock();
        if (l) {
            if (callback)
                callback();
        }
        return l;
    }

    void
    unlock() {
        // TODO(ongardie): apparently try_lock->false...unlock is allowed, but
        // this will then call the callback without the lock, which is unsafe.
        if (callback)
            callback();
        m.unlock();
    }

    native_handle_type
    native_handle() {
        return m.native_handle();
    }

  private:
    /// Underlying mutex.
    std::mutex m;
  public:
    /**
     * This function will be called with the lock held after the lock is
     * acquired and before it is released.
     */
    std::function<void()> callback;

    friend class ConditionVariable;
};

/**
 * Release a mutex upon construction, reacquires it upon destruction.
 * \tparam Mutex
 *      Type of mutex (either std::mutex or Core::Mutex).
 */
template<typename Mutex>
class MutexUnlock {
  public:
    explicit MutexUnlock(std::unique_lock<Mutex>& guard)
        : guard(guard)
    {
        assert(guard.owns_lock());
        guard.unlock();
    }
    ~MutexUnlock()
    {
        guard.lock();
    }
  private:
    std::unique_lock<Mutex>& guard;
};

/**
 * Proof that the caller is holding some mutex.
 * Useful as an additional (unused) argument for some private methods that want
 * to ensure the caller is holding a lock.
 */
class HoldingMutex {
  public:
    /**
     * Constructor from std::lock_guard.
     */
    template<typename Mutex>
    explicit HoldingMutex(const std::lock_guard<Mutex>& lockGuard) {
    }

    /**
     * Constructor from std::unique_lock. Since unique_lock might not, in fact,
     * hold the lock, this uses a dynamic check in the form of an assert().
     */
    template<typename Mutex>
    explicit HoldingMutex(const std::unique_lock<Mutex>& lockGuard) {
        assert(lockGuard.owns_lock());
    }
};

} // namespace LogCabin::Core
} // namespace LogCabin

#endif // LOGCABIN_CORE_MUTEX_H
