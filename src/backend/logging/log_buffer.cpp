/*-------------------------------------------------------------------------
 *
 * log_buffer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/log_buffer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/log_buffer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Buffer
//===--------------------------------------------------------------------===//
size_t LogBuffer::GetSize(){
	return size_;
}

}  // namespace logging
}  // namespace peloton
