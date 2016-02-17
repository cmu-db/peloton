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

#include <cstring>
#include <sys/epoll.h>
#include <unistd.h>

#include "Core/Debug.h"
#include "Event/File.h"
#include "Event/Loop.h"

namespace LogCabin {
namespace Event {

//// class File::Monitor ////

File::Monitor::Monitor(Event::Loop& eventLoop, File& file, uint32_t fileEvents)
    : eventLoop(eventLoop)
    , mutex()
    , file(&file)
{
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = fileEvents;
    event.data.ptr = this->file;
    int r = epoll_ctl(eventLoop.epollfd, EPOLL_CTL_ADD,
                      this->file->fd, &event);
    if (r != 0) {
        PANIC("Adding file %d event with epoll_ctl failed: %s",
              this->file->fd, strerror(errno));
    }
}

File::Monitor::~Monitor()
{
    disableForever();
}

void
File::Monitor::disableForever()
{
    std::lock_guard<std::mutex> mutexGuard(mutex);
    if (file == NULL)
        return;
    Event::Loop::Lock lock(eventLoop);
    int r = epoll_ctl(eventLoop.epollfd, EPOLL_CTL_DEL, file->fd, NULL);
    if (r != 0) {
        PANIC("Removing file %d event with epoll_ctl failed: %s",
              file->fd, strerror(errno));
    }
    file = NULL;
}

void
File::Monitor::setEvents(uint32_t fileEvents)
{
    std::lock_guard<std::mutex> mutexGuard(mutex);
    if (file == NULL)
        return;
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = fileEvents;
    event.data.ptr = file;
    int r = epoll_ctl(eventLoop.epollfd, EPOLL_CTL_MOD, file->fd, &event);
    if (r != 0) {
        PANIC("Modifying file %d event with epoll_ctl failed: %s",
              file->fd, strerror(errno));
    }
}

//// class File ////

File::File(int fd, Ownership ownership)
    : fd(fd)
    , ownership(ownership)
{
}

File::~File()
{
    if (ownership == CLOSE_ON_DESTROY) {
        int r = close(fd);
        if (r != 0)
            PANIC("Could not close file %d: %s", fd, strerror(errno));
    }
}

} // namespace LogCabin::Event
} // namespace LogCabin
