//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// circular_buffer.cpp
//
// Identification: src/container/circular_buffer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/container/circular_buffer.h"

namespace peloton {

// Push a new item
CIRCULAR_BUFFER_TEMPLATE_ARGUMENTS
void CIRCULAR_BUFFER_TYPE::PushBack(ValueType value) {
  circular_buffer_.push_back(value);
}

// Set the container capaciry
CIRCULAR_BUFFER_TEMPLATE_ARGUMENTS
void CIRCULAR_BUFFER_TYPE::SetCapaciry(size_t new_capacity) {
  circular_buffer_.set_capacity(new_capacity);
}

// Returns item count in the circular_buffer
CIRCULAR_BUFFER_TEMPLATE_ARGUMENTS
size_t CIRCULAR_BUFFER_TYPE::GetSize() const { return circular_buffer_.size(); }

// Checks if the circular_buffer is empty
CIRCULAR_BUFFER_TEMPLATE_ARGUMENTS
bool CIRCULAR_BUFFER_TYPE::IsEmpty() const { return circular_buffer_.empty(); }

// Clear all elements
CIRCULAR_BUFFER_TEMPLATE_ARGUMENTS
void CIRCULAR_BUFFER_TYPE::Clear() { circular_buffer_.clear(); }

// Explicit template instantiation
template class CircularBuffer<double>;

}  // namespace peloton
