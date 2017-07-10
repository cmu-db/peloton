//
// Created by tim on 10/07/17.
//
#include <iostream>
#include <string>
#include <memory>
#include "common/harness.h"
#include "common/logger.h"
#include "common/macros.h"
#include "container/lock_free_queue.h"
namespace peloton{
namespace test{

class TaskTests : public PelotonTest {};

void DequeueWrapper(std::shared_ptr<std::string>& tmp, LockFreeQueue<std::shared_ptr<std::string> >& queue){
  queue.Dequeue(tmp);
}

TEST_F(TaskTests, LockFreeQueueTest) {
LockFreeQueue<std::shared_ptr<std::string> > queue(20);
  std::shared_ptr<std::string> p1(new std::string("haha"));
  std::shared_ptr<std::string> p2(new std::string("haha"));
  std::shared_ptr<std::string> p3(new std::string("haha"));

  queue.Enqueue(p1);
  queue.Enqueue(p2);
  queue.Enqueue(p3);

  *p2 = "hoho";
  for (int i = 0; i < 3; ++i) {
    std::shared_ptr<std::string> tmp;
    DequeueWrapper(tmp,queue);
    LOG_DEBUG("string is %s", tmp->c_str);
  }
}
}
}
