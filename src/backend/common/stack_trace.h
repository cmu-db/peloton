//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stack_trace.h
//
// Identification: src/backend/common/stack_trace.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#define BACKWARD_HAS_DW 1

#include "backend/common/backward.h"

namespace peloton {

//===--------------------------------------------------------------------===//
//  Stack Trace
//===--------------------------------------------------------------------===//

class StackTracer {
 public:
  static std::vector<int> make_default_signals();

  StackTracer(const std::vector<int> &signals = make_default_signals());

  bool loaded() const { return _loaded; }

 private:
  backward::details::handle<char *> _stack_content;
  bool _loaded;

  static void sig_handler(int, siginfo_t *info, void *_ctx);
};

void GetStackTrace(int signum = 0);

}  // namespace peloton
