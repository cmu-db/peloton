/* Copyright (c) 2011-2014 Stanford University
 * Copyright (c) 2014-2015 Diego Ongaro
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

#include <mutex>

#ifndef LOGCABIN_EVENT_FILE_H
#define LOGCABIN_EVENT_FILE_H

namespace LogCabin {
namespace Event {

// forward declaration
class Loop;

/**
 * A File is called by the Event::Loop when a file becomes readable or
 * writable. The client should inherit from this and implement the
 * handleFileEvent() method for when the file becomes readable or writable.
 *
 * File objects can be created from any thread, but they will always fire on
 * the thread running the Event::Loop.
 */
class File {
  public:

    /**
     * Registers a File handler to be monitored by the Event::Loop. Once this
     * is constructed, the Event::Loop will call the File's event handler
     * appropriately.
     *
     * This object must be destroyed or disableForever() called BEFORE the File
     * object can be destroyed safely.
     *
     * Details: That Monitor objects have distinct lifetimes from File objects
     * is the key reason why they exist. The registration functionality
     * used to be integrated in File itself, but that posed a problem on
     * destruction. Suppose class MyFile derives from File. ~File() is called
     * after MyFile::handleFileEvent() is no longer safe to call (since MyFile
     * members are already destroyed). So ~File() is too late to block the
     * Event::Loop; it needs to happen before all this. See also
     * https://github.com/logcabin/logcabin/issues/82
     */
    class Monitor {
      public:
        /**
         * Constructor; begins monitoring events on a file.
         * \param eventLoop
         *      Event::Loop that will monitor the File object.
         * \param file
         *      The file to monitor for events.
         * \param fileEvents
         *      The files handler will be invoked when any of the events
         *      specified by this parameter occur (OR-ed combination of
         *      EPOLL_EVENTS values). If this is 0 then the file handler starts
         *      off inactive; it will not trigger until setEvents() has been
         *      called (except possibly for errors such as EPOLLHUP; these
         *      events are always active).
         */
        Monitor(Event::Loop& eventLoop, File& file, uint32_t fileEvents);

        /**
         * Destructor; disables monitoring the file. See disableForever().
         */
        virtual ~Monitor();

        /**
         * Stop monitoring this file. To guarantee that the event loop thread
         * is no longer operating on the File, this method takes an
         * Event::Loop::Lock internally. Once this returns, it is safe to
         * destroy the File object, and it is guaranteed that the event loop
         * thread is no longer operating on the File (unless the caller is
         * running in the context of the File's event handler).
         */
        void disableForever();

        /**
         * Specify the events of interest for the file.
         * This method is safe to call concurrently with file events
         * triggering and disableForever. If called outside an event handler
         * and no Event::Loop::Lock is taken, this may have a short delay while
         * active handlers continue to be called.
         * \param events
         *      Indicates the conditions under which this object should be
         *      invoked (OR-ed combination of EPOLL_EVENTS values). If this is
         *      0 then the file handler is set to inactive; it will not trigger
         *      until setEvents() has been called (except possibly for errors
         *      such as EPOLLHUP; these events are always active).
         */
        void setEvents(uint32_t events);

        /**
         * Event::Loop that will monitor the file.
         */
        Event::Loop& eventLoop;

      private:
        /**
         * Protects #file from concurrent access/modification.
         */
        std::mutex mutex;

        /**
         * Pointer to file being monitored, or NULL if disableForever() has
         * been called.
         */
        File* file;

        // Monitor is not copyable.
        Monitor(const Monitor&) = delete;
        Monitor& operator=(const Monitor&) = delete;
    };

    /**
     * Specifies who is in charge of closing the file descriptor.
     */
    enum Ownership {
        /**
         * The File destructor will close() fd.
         */
        CLOSE_ON_DESTROY,
        /**
         * The caller will be in charge of closing fd. The File destructor will
         * not close() fd.
         */
        CALLER_CLOSES_FD,
    };

    /**
     * Construct a file event handler. After this is constructed, you'll want
     * to create an Event::File::Monitor for it.
     * \param fd
     *      A file descriptor of a valid open file to monitor. Unless release()
     *      is called first, this File object will close the file descriptor
     *      when it is destroyed.
     * \param ownership
     *      Specifies who is in charge of closing fd. Default: this class will
     *      close fd in the destructor.
     */
    explicit File(int fd, Ownership ownership = CLOSE_ON_DESTROY);

    /**
     * Destructor. Closes the file descriptor if ownership is set to
     * CLOSE_ON_DESTROY.
     */
    virtual ~File();

    /**
     * This method is overridden by a subclass and invoked when a file event
     * occurs. This method will be invoked by the main event loop on
     * whatever thread is running the Event::Loop.
     *
     * If the event still exists when this method returns (e.g., the file is
     * readable but the method did not read the data), then the method will be
     * invoked again (unless flags such as EPOLLONESHOT or EPOLLET are used).
     *
     * \param events
     *      Indicates whether the file is readable or writable or both (OR'ed
     *      combination of EPOLL_EVENTS values).
     */
    virtual void handleFileEvent(uint32_t events) = 0;

    /**
     * The file descriptor to monitor.
     */
    const int fd;

    /**
     * See Ownership.
     */
    const Ownership ownership;

    // File is not copyable.
    File(const File&) = delete;
    File& operator=(const File&) = delete;
};

} // namespace LogCabin::Event
} // namespace LogCabin

#endif /* LOGCABIN_EVENT_FILE_H */
