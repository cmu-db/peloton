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

#ifndef LOGCABIN_EVENT_SIGNAL_H
#define LOGCABIN_EVENT_SIGNAL_H

#include "Event/File.h"

namespace LogCabin {
namespace Event {

// forward declaration
class Loop;

/**
 * A Signal is called by the Event::Loop when a Unix signal is received.
 * The client should inherit from this and implement the handleSignalEvent()
 * method for when the signal is received. This is persistent; it will handle
 * repeated delivery of the signal.
 *
 * Signal handlers can be created from any thread, but they will always fire on
 * the thread running the Event::Loop.
 */
class Signal : private Event::File {
  public:
    /**
     * Blocks asynchronous signal delivery on the current thread for the given
     * signal. This should normally be called before any secondary threads are
     * started, so that all subsequent threads also have the signal blocked.
     * \warning
     *      This class is not thread-safe: the caller must use an external
     *      mutex or (more commonly) may only manipulate this class while
     *      a single thread is running.
     */
    class Blocker {
      public:
        /**
         * Constructor.
         * Masks asynchronous signal delivery for signalNumber.
         */
        explicit Blocker(int signalNumber);
        /**
         * Destructor. Unmasks asynchronous signal delivery for signalNumber.
         */
        ~Blocker();
        /**
         * Blocks further signals if they are not already blocked.
         */
        void block();
        /**
         * Leave the signal blocked when this object is destroyed.
         */
        void leaveBlocked();
        /**
         * Unblocks signals if they are blocked. Also clears
         * #shouldLeaveBlocked.
         */
        void unblock();
        const int signalNumber;
      private:
        /**
         * Set to true if the signal is currently blocked.
         */
        bool isBlocked;
        /**
         * Set to true if the signal should be left unblocked on destruction.
         */
        bool shouldLeaveBlocked;
    };

    /**
     * Registers a Signal handler to be monitored by the Event::Loop. Once this
     * is constructed, the Event::Loop will call the Signal's event handler
     * appropriately.
     *
     * This object must be destroyed or disableForever() called BEFORE the
     * Signal object can be destroyed safely.
     */
    class Monitor : private File::Monitor {
      public:
        /// See File::Monitor::Monitor.
        Monitor(Event::Loop& eventLoop, Signal& signal);
        /// See File::Monitor::~Monitor.
        ~Monitor();
        /// See File::Monitor::disableForever.
        using File::Monitor::disableForever;
    };

    /**
     * Construct a signal handler.
     * See also Blocker, which you'll generally need to create first,
     * and Monitor, which you'll need to create after.
     * \param signalNumber
     *      The signal number identifying which signal to receive
     *      (man signal.h).
     */
    explicit Signal(int signalNumber);

    /**
     * Destructor.
     */
    virtual ~Signal();

    /**
     * This method is overridden by a subclass and invoked when the signal
     * is received. This method will be invoked by the main event loop on
     * whatever thread is running the Event::Loop.
     */
    virtual void handleSignalEvent() = 0;

    /**
     * The signal number identifying which signal to receive (man signal.h).
     */
    const int signalNumber;

  private:
    /**
     * Generic event handler for files. Calls handleSignalEvent().
     */
    void handleFileEvent(uint32_t events);

    // Signal is not copyable.
    Signal(const Signal&) = delete;
    Signal& operator=(const Signal&) = delete;
};

} // namespace LogCabin::Event
} // namespace LogCabin

#endif /* LOGCABIN_EVENT_SIGNAL_H */
