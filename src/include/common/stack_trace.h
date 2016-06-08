//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stack_trace.h
//
// Identification: src/include/common/stack_trace.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

namespace peloton {

void RegisterSignalHandlers();

void PrintStackTrace();

void SignalHandler(int signum);

}  // End peloton namespace
