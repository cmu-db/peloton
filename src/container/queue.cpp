//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// queue.cpp
//
// Identification: src/container/queue.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "container/queue.h"
#include "common/types.h"

namespace peloton {

QUEUE_TEMPLATE_ARGUMENTS
QUEUE_TYPE::Queue(const size_t& size) : queue_(size) {
}

QUEUE_TEMPLATE_ARGUMENTS
void QUEUE_TYPE::Enqueue(ValueType& item) {
  queue_.enqueue(item);
}

QUEUE_TEMPLATE_ARGUMENTS
bool QUEUE_TYPE::Dequeue(ValueType& item) {
  return queue_.try_dequeue(item);
}

// Explicit template instantiation
template class Queue<TupleMetadata>;

}  // namespace peloton
