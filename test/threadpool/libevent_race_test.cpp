//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool_test.cpp
//
// Identification: test/threadpool/worker_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <thread>
#include <cstdio>
#include <unistd.h>

#include "common/harness.h"
#include "common/logger.h"
#include "common/macros.h"
#include "threadpool/worker.h"
#include "threadpool/task.h"
#include "event.h"

#define CALL_NUM 10
#define EXPECT_NOTNULL(pointer) EXPECT_TRUE(pointer != NULL);

//===--------------------------------------------------------------------===//
// WorkerPool Tests
//===--------------------------------------------------------------------===//

namespace peloton {
namespace test {

class WorkerPoolTests : public PelotonTest {};

void shortTask(void* param) {
  int* num = (int *)param;
  (*num)--;
  usleep(100000); // num * 0.1s
}

void longTask(void* param) {
  int* num = (int *)param;
  (*num)--;
  usleep(1000000); // num seconds.
}


// Callback function for MyLibeventThread
void eventCallback(evutil_socket_t fd, short evflags, void *args);

class MyLibeventThread {
 private:
  struct event_base *libevent_base_;
  int send_fd_;
  int receive_fd_;
  struct event *event_;
  threadpool::TaskQueue *task_queue_;
 public:
  MyLibeventThread(threadpool::TaskQueue *task_queue) : task_queue_(task_queue) {
    int fds[2];
    if (pipe(fds)) {
      LOG_ERROR("Can't create notify pipe to accept connections");
      exit(1);
    }
    send_fd_ = fds[1];
    receive_fd_ = fds[0];
    libevent_base_ = event_base_new();
    event_ = event_new(libevent_base_, receive_fd_, EV_READ | EV_PERSIST,
                       eventCallback, this);
    event_add(event_, 0);
    LOG_INFO("Libevent thread adds read event");
  }
  void startMyLibeventThread() {
    LOG_INFO("Libevent thread starts listening on event");
    event_base_dispatch(libevent_base_);
  }
  int getSendfd() {
    return send_fd_;
  }
  threadpool::TaskQueue *getTaskQueue() {
    return task_queue_;
  }
  struct event_base *getEventBase() {
    return libevent_base_;
  }
  struct event *getEvent() {
    return event_;
  }
};

void eventCallback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                   UNUSED_ATTRIBUTE short evflags, void *args) {
  LOG_INFO("----- master activate");
  MyLibeventThread *thread = static_cast<MyLibeventThread *>(args);
  threadpool::TaskQueue *tq = thread->getTaskQueue();
  char m_buf[1];
  if (read(fd, m_buf, 1) != 1) {
    LOG_ERROR("Can't read from the libevent pipe");
    return;
  }
  static char count = m_buf[0];
  static char start = m_buf[0];
  LOG_INFO("master read: %c, count: %c, start: %c", m_buf[0], count, start);
  EXPECT_EQ(m_buf[0], count);

  // exit the event loop if test completes
  if (m_buf[0] == (start + CALL_NUM - 1)) {
    event_base_loopexit(thread->getEventBase(), NULL);
    return;
  }
  // submit tasks into TaskQueue
  std::vector<std::shared_ptr<threadpool::Task>> task_v;
  std::vector<int> params;
  int task_num = 2, i = 0;
  for (i = 0; i < task_num; i++) {
    params.push_back(1);
  }
  for(i = 0; i < task_num; i++) {
    if (i % 2 == 0) {
      task_v.push_back(std::make_shared<threadpool::Task>(longTask, &params.at(i)));
    } else {
      task_v.push_back(std::make_shared<threadpool::Task>(shortTask, &params.at(i)));
    }
  }
  tq->SubmitTaskBatch(task_v);

  count++;
  LOG_INFO("master completes callback, count: %c", count);
}


// callerThread attempts to activate master thread every 0.06s.
void callerFunc(MyLibeventThread *thread) {
  int i = 0;
  char buf[CALL_NUM];
  for (i = 0; i < CALL_NUM; i++) {
    buf[i] = 'A' + i;
  }
  for (i = 0; i < CALL_NUM; i++) {
    if (write(thread->getSendfd(), &(buf[i]), 1)) {
      LOG_INFO("caller attempts to activate network thread");
      usleep(60000);
    } else {
      EXPECT_EQ(false, true);
    }
  }
  LOG_INFO("caller exits");
}

class CallerThread {
 public:
  CallerThread(MyLibeventThread *thread) : thread_(thread) {
    caller_ = std::thread(callerFunc, thread_);
    caller_.detach();
  }

 private:
  MyLibeventThread *thread_;
  std::thread caller_;
};

// For this test, libevent thread should be only activated by the callerThread
// after its previous tasks are completed by worker threads.
TEST_F(WorkerPoolTests, LibeventActivateTest) {
  const size_t queue_size = 50;
  const size_t pool_size = 4;
  threadpool::TaskQueue tq(queue_size);
  threadpool::WorkerPool wp(pool_size, &tq);
  MyLibeventThread libeventThread(&tq);
  CallerThread callerThread(&libeventThread);
  libeventThread.startMyLibeventThread();

  event_free(libeventThread.getEvent());
  event_base_free(libeventThread.getEventBase());

  wp.Shutdown();
}

} // end of test
} // end of peloton