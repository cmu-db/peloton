/*-------------------------------------------------------------------------
 *
 * circular_buffer_pool.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/circular_buffer_pool.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <atomic>
#include "backend/logging/circular_buffer_pool.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Circular Buffer Pool
//===--------------------------------------------------------------------===//
template <unsigned int Capacity>
bool CircularBufferPool<Capacity>::Put(std::shared_ptr<LogBuffer>){
	return true;
}

template <unsigned int Capacity>
std::shared_ptr<LogBuffer> CircularBufferPool<Capacity>::Get(){
	return std::shared_ptr<LogBuffer>(nullptr);
}

}  // namespace logging
}  // namespace peloton
