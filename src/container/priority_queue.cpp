//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// queue.cpp
//
// Identification: src/common/queue.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/priority_queue.h"

namespace peloton {

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
PRIORITY_QUEUE_TYPE::PriorityQueue(const size_t capacity) :
priority_queue(capacity) {
}

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
PRIORITY_QUEUE_TYPE::~PriorityQueue(){
}

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
bool PRIORITY_QUEUE_TYPE::Push(const ValueType v){
  return priority_queue.push(v);
}

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
bool PRIORITY_QUEUE_TYPE::Pop(ValueType &v){
  return priority_queue.pop(v);
}

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
void PRIORITY_QUEUE_TYPE::Clear(){
  priority_queue.clear();
}

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
bool PRIORITY_QUEUE_TYPE::IsEmpty() const{
  return priority_queue.empty();
}

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
bool PRIORITY_QUEUE_TYPE::IsFull() const{
  return priority_queue.full();
}

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
size_t  PRIORITY_QUEUE_TYPE::GetCapacity() const{
  return priority_queue.capacity();
}

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
size_t PRIORITY_QUEUE_TYPE::GetSize() const{
  return priority_queue.size();
}


// Explicit template instantiation
template class PriorityQueue<uint32_t>;

}  // End peloton namespace
