/* Copyright (c) 2012-2014 Stanford University
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

#include <gtest/gtest.h>
#include <sys/epoll.h>
#include <unistd.h>

#include "Event/File.h"
#include "Event/Loop.h"

namespace LogCabin {
namespace Event {
namespace {

struct MyFile : public Event::File {
    explicit MyFile(Event::Loop& loop,
                    int fd,
                    Ownership ownership = CLOSE_ON_DESTROY)
        : File(fd, ownership)
        , eventLoop(loop)
        , triggerCount(0)
    {
    }
    void handleFileEvent(uint32_t events) {
        ++triggerCount;
        eventLoop.exit();
    }
    Event::Loop& eventLoop;
    uint32_t triggerCount;
};

struct EventFileTest : public ::testing::Test {
    EventFileTest()
        : loop()
        , pipeFds()
    {
        EXPECT_EQ(0, pipe(pipeFds));
    }
    ~EventFileTest()
    {
        closePipeFds();
    }

    void
    closePipeFds()
    {
        Event::Loop::Lock lock(loop);
        if (pipeFds[0] >= 0) {
            EXPECT_EQ(0, close(pipeFds[0]));
            pipeFds[0] = -1;
        }
        if (pipeFds[1] >= 0) {
            EXPECT_EQ(0, close(pipeFds[1]));
            pipeFds[1] = -1;
        }
    }
    bool hasPending() {
        struct epoll_event event;
        int r = epoll_wait(loop.epollfd, &event,
                           1, 0);
        if (r == 0)
            return false;
        return true;
    }
    Event::Loop loop;
    int pipeFds[2];
};


TEST_F(EventFileTest, basics) {
    MyFile file(loop, pipeFds[0]);
    pipeFds[0] = -1;
    File::Monitor monitor(loop, file, EPOLLIN|EPOLLONESHOT);
    EXPECT_EQ(1, write(pipeFds[1], "x", 1));
    loop.runForever();
    EXPECT_EQ(1U, file.triggerCount);
}

TEST_F(EventFileTest, Monitor_disableForever) {
    MyFile file(loop, pipeFds[0]);
    pipeFds[0] = -1;
    File::Monitor monitor(loop, file, EPOLLIN|EPOLLONESHOT);
    monitor.disableForever();
    monitor.disableForever();
    EXPECT_TRUE(NULL == monitor.file);
    EXPECT_EQ(1, write(pipeFds[1], "x", 1));
    EXPECT_FALSE(hasPending());
}

TEST_F(EventFileTest, Monitor_setEvents) {
    MyFile file(loop, pipeFds[0]);
    File::Monitor monitor(loop, file, 0);
    pipeFds[0] = -1;
    EXPECT_EQ(1, write(pipeFds[1], "x", 1));
    EXPECT_FALSE(hasPending());
    monitor.setEvents(EPOLLIN|EPOLLONESHOT);
    loop.runForever();
    EXPECT_EQ(1U, file.triggerCount);
    file.triggerCount = 0;
    monitor.setEvents(0);
    EXPECT_FALSE(hasPending());
}

TEST_F(EventFileTest, Monitor_setEventsAfterDestroy) {
    std::unique_ptr<MyFile> file(new MyFile(loop, pipeFds[0]));
    pipeFds[0] = -1;
    File::Monitor monitor(loop, *file, EPOLLIN|EPOLLONESHOT);
    monitor.disableForever();
    file.reset();
    monitor.setEvents(EPOLLOUT|EPOLLONESHOT);
}

TEST_F(EventFileTest, File_destructor_close_on_destroy) {
    {
        MyFile file(loop, pipeFds[0]);
        File::Monitor monitor(loop, file, EPOLLIN|EPOLLONESHOT);
    }
    EXPECT_EQ(-1, close(pipeFds[0]));
    EXPECT_EQ(EBADF, errno);
    pipeFds[0] = -1;
}

TEST_F(EventFileTest, File_destructor_caller_closes_fd) {
    {
        MyFile file(loop, pipeFds[0], MyFile::CALLER_CLOSES_FD);
        File::Monitor monitor(loop, file, EPOLLIN|EPOLLONESHOT);
    }
    // closePipeFds() makes sure pipeFds[0] can be closed without errors
}

} // namespace LogCabin::Event::<anonymous>
} // namespace LogCabin::Event
} // namespace LogCabin
