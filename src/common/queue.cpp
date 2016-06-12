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

template<typename ValueType>
Queue<ValueType>::Queue(const size_t capacity) :
priority_queue(capacity) {
}

template<typename ValueType>
Queue<ValueType>::~Queue(){
}

template<typename ValueType>
bool Queue<ValueType>::Push(const ValueType v){
  return priority_queue.push(v);
}

template<typename ValueType>
bool Queue<ValueType>::Pop(ValueType &v){
  return priority_queue.pop(v);
}

template<typename ValueType>
void Queue<ValueType>::Clear(){
  priority_queue.clear();
}

template<typename ValueType>
bool Queue<ValueType>::IsEmpty() const{
  return priority_queue.empty();
}

template<typename ValueType>
bool Queue<ValueType>::IsFull() const{
  return priority_queue.full();
}

template<typename ValueType>
size_t  Queue<ValueType>::GetCapacity() const{
  return priority_queue.capacity();
}

template<typename ValueType>
size_t Queue<ValueType>::GetSize() const{
  return priority_queue.size();
}


// Explicit template instantiation
template class Queue<uint32_t>;

}  // End peloton namespace
