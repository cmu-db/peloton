/* Copyright (c) 2011-2014 Stanford University
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

#ifndef LOGCABIN_EVENT_LOOP_H
#define LOGCABIN_EVENT_LOOP_H

#include <cinttypes>
#include <memory>

#include "Core/ConditionVariable.h"
#include "Core/Mutex.h"
#include "Event/Timer.h"

namespace LogCabin {
namespace Event {

// forward declarations
class File;

/**
 * This class contains an event loop based on Linux's epoll interface.
 * It keeps track of interesting events such as timers and socket activity, and
 * arranges for callbacks to be invoked when the events happen.
 */
class Loop {
  public:

    /**
     * Lock objects are used to synchronize between the Event::Loop thread and
     * other threads.  As long as a Lock object exists the following guarantees
     * are in effect: either
     * (a) the thread is the event loop thread or
     * (b) no other thread has a Lock object and the event loop thread has
     *     paused in a safe place (with no event handlers active) waiting for
     *     the Lock to be destroyed.
     * Locks may be used recursively.
     */
    class Lock {
      public:
        /// Constructor. Acquire the lock.
        explicit Lock(Event::Loop& eventLoop);
        /// Destructor. Release the lock.
        ~Lock();
        /// Event::Loop to lock.
        Event::Loop& eventLoop;
      private:
        // Lock is not copyable.
        Lock(const Lock&) = delete;
        Lock& operator=(const Lock&) = delete;
    };

    /**
     * Constructor.
     */
    Loop();

    /**
     * Destructor. The caller must ensure that no events still exist and that
     * the event loop is not running.
     */
    ~Loop();

    /**
     * Run the main event loop until exit() is called.
     * It is safe to call this again after it returns.
     * The caller must ensure that only one thread is executing runForever() at
     * a time.
     */
    void runForever();

    /**
     * Exit the main event loop, if one is running. It may return before
     * runForever() has returned but guarantees runForever() will return soon.
     *
     * If the event loop is not running, then the next time it runs, it will
     * exit right away (these semantics can be useful to avoid races).
     *
     * This may be called from an event handler or from any thread.
     */
    void exit();

  private:

    /**
     * Used in breakTimer, whose purpose is not to handle events but to break
     * runForever() out of epoll_wait.
     */
    class NullTimer : public Event::Timer {
      public:
        void handleTimerEvent();
    };

    /**
     * The file descriptor used in epoll calls to monitor other files.
     */
    int epollfd;

    /**
     * Used by Event::Loop::Lock to break runForever() out of epoll_wait.
     */
    NullTimer breakTimer;

    /**
     * This is a flag to runForever() to exit, set by exit().
     * Protected by Event::Loop::Lock (or #mutex directly inside runForever()).
     */
    bool shouldExit;

    /**
     * This mutex protects all of the members of this class defined below this
     * point, except breakTimerMonitor.
     */
    std::mutex mutex;

    /**
     * The thread ID of the thread running the event loop, or
     * Core::ThreadId::NONE if no thread is currently running the event loop.
     * This serves two purposes:
     * First, it allows Lock to tell whether it's running under the event loop.
     * Second, it allows Lock to tell if the event loop is running.
     */
    uint64_t runningThread;

    /**
     * The number of Lock instances, including those that are blocked and those
     * that are active.
     * runForever() waits for this to drop to 0 before running again.
     */
    uint32_t numLocks;

    /**
     * The number of Locks that are active. This is used to support reentrant
     * Lock objects, specifically to know when to set #lockOwner back to
     * Core::ThreadId::NONE.
     */
    uint32_t numActiveLocks;

    /**
     * The thread ID of the thread with active Locks, or Core::ThreadId::NONE
     * if no thread currently has a Lock. This allows for mutually exclusive
     * yet reentrant Lock objects.
     */
    uint64_t lockOwner;

    /**
     * Signaled when it may be safe for a Lock constructor to complete. This
     * happens either because runForever() just reached its safe place or
     * because some other Lock was destroyed.
     */
    Core::ConditionVariable safeToLock;

    /**
     * Signaled when there are no longer any Locks active.
     */
    Core::ConditionVariable unlocked;

#if 1
    /**
     * Lockable type that compiles out entirely.
     */
    struct NoOpLockable {
        void lock() {}
        void unlock() {}
    } extraMutexToSatisfyRaceDetector;
#else
    /**
     * Race detectors tend to know about pthreads locks and not much else. This
     * is a pthread lock that is acquired at the bottom of Lock's constructor
     * at the top of Lock's destructor, and also used by Loop::runForever(),
     * for the purpose of asserting to race detectors that there is a mechanism
     * in place for mutual exclusion.
     */
    std::recursive_mutex extraMutexToSatisfyRaceDetector;
#endif

    /**
     * Watches breakTimer for events.
     */
    Event::Timer::Monitor breakTimerMonitor;

    friend class Event::File;

    // Loop is not copyable.
    Loop(const Loop&) = delete;
    Loop& operator=(const Loop&) = delete;
}; // class Loop

} // namespace LogCabin::Event
} // namespace LogCabin

#endif /* LOGCABIN_EVENT_LOOP_H */
