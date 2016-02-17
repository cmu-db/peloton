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

#include <cstring>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include "Core/Debug.h"
#include "Event/Loop.h"
#include "Event/Signal.h"

namespace LogCabin {
namespace Event {

namespace {

/// Helper for Signal constructor.
int
createSignalFd(int signalNumber)
{
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, signalNumber);
    int fd = signalfd(-1, &mask, SFD_NONBLOCK|SFD_CLOEXEC);
    if (fd < 0) {
        PANIC("Could not create signalfd for signal %d: %s",
              signalNumber, strerror(errno));
    }
    return fd;
}

} // anonymous namespace

//// class Signal::Blocker ////

Signal::Blocker::Blocker(int signalNumber)
    : signalNumber(signalNumber)
    , isBlocked(false)
    , shouldLeaveBlocked(false)
{
    block();
}

Signal::Blocker::~Blocker()
{
    if (!shouldLeaveBlocked)
        unblock();
}

void
Signal::Blocker::block()
{
    if (!isBlocked) {
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, signalNumber);
        int r = pthread_sigmask(SIG_BLOCK, &mask, NULL);
        if (r != 0) {
            PANIC("Could not block signal %d: %s",
                  signalNumber, strerror(r));
        }
        isBlocked = true;
    }
}

void
Signal::Blocker::leaveBlocked()
{
    shouldLeaveBlocked = true;
}

void
Signal::Blocker::unblock()
{
    if (isBlocked) {
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, signalNumber);
        int r = pthread_sigmask(SIG_UNBLOCK, &mask, NULL);
        if (r != 0) {
            PANIC("Could not unblock signal %d: %s",
                  signalNumber, strerror(r));
        }
        isBlocked = false;
        shouldLeaveBlocked = false;
    }
}


//// class Signal::Monitor ////

Signal::Monitor::Monitor(Event::Loop& eventLoop, Signal& signal)
    : File::Monitor(eventLoop, signal, EPOLLIN)
{
}

Signal::Monitor::~Monitor()
{
}


//// class Signal ////

Signal::Signal(int signalNumber)
    : Event::File(createSignalFd(signalNumber))
    , signalNumber(signalNumber)
{
}

Signal::~Signal()
{
}

void
Signal::handleFileEvent(uint32_t events)
{
    struct signalfd_siginfo info;
    ssize_t s = read(fd, &info, sizeof(struct signalfd_siginfo));
    if (s < 0) {
        PANIC("Could not read signal info (to discard): %s",
              strerror(errno));
    }
    if (size_t(s) != sizeof(struct signalfd_siginfo)) {
        PANIC("Could not read full signal info (to discard)");
    }
    handleSignalEvent();
}

} // namespace LogCabin::Event
} // namespace LogCabin
