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

#ifndef LOGCABIN_EVENT_TIMER_H
#define LOGCABIN_EVENT_TIMER_H

#include "Core/Time.h"
#include "Event/File.h"

namespace LogCabin {
namespace Event {

// forward declaration
class Loop;

/**
 * A Timer is called by the Event::Loop when time has elapsed.
 * The client should inherit from this and implement the trigger method for
 * when the timer expires.
 *
 * Timers can be added and scheduled from any thread, but they will always fire
 * on the thread running the Event::Loop.
 */
class Timer : private File {
  public:

    /**
     * Registers a Timer handler to be monitored by the Event::Loop. Once this
     * is constructed, the Event::Loop will call the Timer's event handler
     * appropriately.
     *
     * This object must be destroyed or disableForever() called BEFORE the
     * Timer object can be destroyed safely.
     */
    class Monitor : private File::Monitor {
      public:
        /// See File::Monitor::Monitor.
        Monitor(Event::Loop& eventLoop, Timer& timer);
        /// See File::Monitor::~Monitor.
        ~Monitor();
        /// See File::Monitor::disableForever.
        using File::Monitor::disableForever;
    };

    /**
     * Construct a timer but do not schedule it to trigger.
     * After this is constructed, you'll want to create an
     * Event::Timer::Monitor for it and also schedule() it, in either order.
     */
    Timer();

    /**
     * Destructor.
     */
    virtual ~Timer();

    /**
     * This method is overridden by a subclass and invoked when the timer
     * expires. This method will be invoked by the main event loop on whatever
     * thread is running the Event::Loop.
     */
    virtual void handleTimerEvent() = 0;

    /**
     * Start the timer.
     * \param nanoseconds
     *     The timer will trigger once this number of nanoseconds have elapsed.
     *     If the timer was already scheduled, the old time is forgotten.
     */
    void schedule(uint64_t nanoseconds);

    /**
     * Start the timer for an absolute point in time. If the timeout is in the
     * past, the timer will trigger immediately once the event loop runs.
     * \param timeout
     *     The timer will trigger once this timeout has past.
     *     If the timer was already scheduled, the old time is forgotten.
     */
    void scheduleAbsolute(Core::Time::SteadyClock::time_point timeout);

    /**
     * Stop the timer from calling handleTimerEvent().
     * This will cancel the current timer, if any, so that handleTimerEvent()
     * is not called until the next time it is scheduled.
     *
     * This method behaves in a friendly way when called concurrently with the
     * timer firing:
     * - If this method is called from another thread and handleTimerEvent() is
     *   running concurrently on the event loop thread, deschedule() will wait
     *   for handleTimerEvent() to complete before returning.
     * - If this method is called from the event loop thread and
     *   handleTimerEvent() is currently being fired, then deschedule() can't
     *   wait and returns immediately.
     */
    void deschedule();

  private:
    /**
     * Returns true if the timer has been scheduled and has not yet fired.
     * \warning
     *      This is prone to races; it's primarily useful for unit tests.
     *      It also improperly returns false after scheduleAbsolute() is called
     *      with a time that has expired. It's marked as private so it can't be
     *      accessed from normal (non-test) code.
     *      TODO(ongaro): remove entirely or add additional state to this
     *      class to make this work correctly?
     * \return
     *      True if the timer has been scheduled and will eventually call
     *      handleTimerEvent().
     *      False if the timer is currently running handleTimerEvent() or if it
     *      is not scheduled to call handleTimerEvent() in the future.
     */
    bool isScheduled() const;

    /**
     * Generic event handler for files. Calls handleTimerEvent().
     */
    void handleFileEvent(uint32_t events);

    // Timer is not copyable.
    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;
};

} // namespace LogCabin::Event
} // namespace LogCabin

#endif /* LOGCABIN_EVENT_TIMER_H */
