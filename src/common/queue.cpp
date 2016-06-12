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

#include "common/queue.h"

namespace peloton {

QUEUE_TEMPLATE_ARGUMENTS
QUEUE_TYPE::Queue(const size_t capacity) :
priority_queue(capacity) {
}

QUEUE_TEMPLATE_ARGUMENTS
QUEUE_TYPE::~Queue(){
}

QUEUE_TEMPLATE_ARGUMENTS
bool QUEUE_TYPE::Push(const ValueType v){
  return priority_queue.push(v);
}

QUEUE_TEMPLATE_ARGUMENTS
bool QUEUE_TYPE::Pop(ValueType &v){
  return priority_queue.pop(v);
}

QUEUE_TEMPLATE_ARGUMENTS
void QUEUE_TYPE::Clear(){
  priority_queue.clear();
}

QUEUE_TEMPLATE_ARGUMENTS
bool QUEUE_TYPE::IsEmpty() const{
  return priority_queue.empty();
}

QUEUE_TEMPLATE_ARGUMENTS
bool QUEUE_TYPE::IsFull() const{
  return priority_queue.full();
}

QUEUE_TEMPLATE_ARGUMENTS
size_t  QUEUE_TYPE::GetCapacity() const{
  return priority_queue.capacity();
}

QUEUE_TEMPLATE_ARGUMENTS
size_t QUEUE_TYPE::GetSize() const{
  return priority_queue.size();
}


// Explicit template instantiation
template class Queue<uint32_t>;

}  // End peloton namespace
