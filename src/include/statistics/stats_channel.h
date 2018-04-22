//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_stats_channel.h
//
// Identification: src/statistics/abstract_stats_channel.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/container/lock_free_queue.h"

namespace peloton {
namespace stats {

template <typename Message, typename Reducer>
class StatsChannel {
 public:
  StatsChannel(size_t capacity) : channel_(capacity) {}

  inline void AddMessage(Message message) { channel_.Enqueue(message); }

  inline void Reduce(Reducer &r) {
    Message message;
    while (channel_.Dequeue(message)) {
      r.Consume(message);
    }
  }

 private:
  LockFreeQueue<Message> channel_;
};

}  // namespace stats
}  // namespace peloton