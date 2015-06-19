/*-------------------------------------------------------------------------
 *
 * kernel.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/backend/kernel.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/types.h"

namespace nstore {
namespace backend {

//===--------------------------------------------------------------------===//
// Kernel
//===--------------------------------------------------------------------===//

// Main handler for query
class Kernel {

 public:

  static ResultType Handler(const char* query);

  static int GetTableList(int arg);

};

// Wrapper functions declarations
//extern "C" {
//
//void *Kernel_Create();
//void Kernel_Destroy(void *obj);
//int Kernel_GetTableList(Kernel* kernel, int arg);
//
//}


} // namespace backend
} // namespace nstore
