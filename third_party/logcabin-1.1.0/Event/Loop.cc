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

#include <cassert>
#include <cstring>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include "Core/Debug.h"
#include "Core/ThreadId.h"
#include "Event/File.h"
#include "Event/Loop.h"
#include "Event/Timer.h"

namespace LogCabin {
namespace Event {

namespace {

/// Helper for Loop constructor.
int
createEpollFd()
{
    int epollfd = epoll_create1(0);
    if (epollfd < 0)
        PANIC("epoll_create1 failed: %s", strerror(errno));
    return epollfd;
}

} // anonymous namespace

////////// Loop::Lock //////////

Loop::Lock::Lock(Event::Loop& eventLoop)
    : eventLoop(eventLoop)
{
    {
    std::unique_lock<std::mutex> lockGuard(eventLoop.mutex);
    ++eventLoop.numLocks;
    if (eventLoop.runningThread != Core::ThreadId::getId() &&
        eventLoop.lockOwner != Core::ThreadId::getId()) {
        // This is an actual lock: we're not running inside the event loop, and
        //                         we're not recursively locking.
        if (eventLoop.runningThread != Core::ThreadId::NONE)
            eventLoop.breakTimer.schedule(0);
        while (eventLoop.runningThread != Core::ThreadId::NONE ||
               eventLoop.lockOwner != Core::ThreadId::NONE) {
            eventLoop.safeToLock.wait(lockGuard);
        }
        // Take ownership of the lock
        eventLoop.lockOwner = Core::ThreadId::getId();
    }
    ++eventLoop.numActiveLocks;
    }
    eventLoop.extraMutexToSatisfyRaceDetector.lock();
}

Loop::Lock::~Lock()
{
    eventLoop.extraMutexToSatisfyRaceDetector.unlock();
    std::lock_guard<std::mutex> lockGuard(eventLoop.mutex);
    --eventLoop.numLocks;
    --eventLoop.numActiveLocks;
    if (eventLoop.numActiveLocks == 0) {
        eventLoop.lockOwner = Core::ThreadId::NONE;
        if (eventLoop.numLocks == 0)
            eventLoop.unlocked.notify_one();
        else
            eventLoop.safeToLock.notify_one();
    }
}

////////// Loop::NullTimer //////////

void
Loop::NullTimer::handleTimerEvent()
{
    // do nothing
}

////////// Loop //////////

Loop::Loop()
    : epollfd(createEpollFd())
    , breakTimer()
    , shouldExit(false)
    , mutex()
    , runningThread(Core::ThreadId::NONE)
    , numLocks(0)
    , numActiveLocks(0)
    , lockOwner(Core::ThreadId::NONE)
    , safeToLock()
    , unlocked()
    , extraMutexToSatisfyRaceDetector()
    , breakTimerMonitor(*this, breakTimer)
{
}

Loop::~Loop()
{
    breakTimerMonitor.disableForever();
    if (epollfd >= 0) {
        int r = close(epollfd);
        if (r != 0)
            PANIC("Could not close epollfd %d: %s", epollfd, strerror(errno));
    }
}

void
Loop::runForever()
{
    while (true) {
        { // Handle Loop::Lock requests and exiting.
            std::unique_lock<std::mutex> lockGuard(mutex);
            runningThread = Core::ThreadId::NONE;
            // Wait for all Locks to finish up
            while (numLocks > 0) {
                safeToLock.notify_one();
                unlocked.wait(lockGuard);
            }
            if (shouldExit) {
                shouldExit = false;
                return;
            }
            runningThread = Core::ThreadId::getId();
        }
        std::unique_lock<decltype(extraMutexToSatisfyRaceDetector)>
            extraLockGuard(extraMutexToSatisfyRaceDetector);

        // Block in the kernel for events, then process them.
        // TODO(ongaro): It'd be more efficient to handle more than 1 event at
        // a time, but that complicates the interface: if a handler removes
        // itself from the poll set and deletes itself, we don't want further
        // events to call that same handler. For example, if a socket is
        // dup()ed so that the receive side handles events separately from the
        // send side, both are active events, and the first deletes the object,
        // this could cause trouble.
        enum { NUM_EVENTS = 1 };
        struct epoll_event events[NUM_EVENTS];
        int r = epoll_wait(epollfd, events, NUM_EVENTS, -1);
        if (r <= 0) {
            if (errno == EINTR) // caused by GDB
                continue;
            PANIC("epoll_wait failed: %s", strerror(errno));
        }
        for (int i = 0; i < r; ++i) {
            Event::File& file = *static_cast<Event::File*>(events[i].data.ptr);
            file.handleFileEvent(events[i].events);
        }
    }
}

void
Loop::exit()
{
    Event::Loop::Lock lock(*this);
    shouldExit = true;
}

} // namespace LogCabin::Event
} // namespace LogCabin
