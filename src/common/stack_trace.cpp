//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stack_trace.cpp
//
// Identification: src/common/stack_trace.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#define UNW_LOCAL_ONLY
#include <execinfo.h>
#include <errno.h>
#include <cxxabi.h>
#include <signal.h>
#include <libunwind.h>
#include <cstdio>
#include <cstdlib>

#include "common/logger.h"
#include "common/stack_trace.h"

namespace peloton {

void SignalHandler(int signum) {
  // associate each signal with a signal name string.
  const char* name = NULL;
  switch (signum) {
    case SIGABRT:
      name = "SIGABRT";
      break;
    case SIGSEGV:
      name = "SIGSEGV";
      break;
    case SIGBUS:
      name = "SIGBUS";
      break;
    case SIGILL:
      name = "SIGILL";
      break;
    case SIGFPE:
      name = "SIGFPE";
      break;
  }

  // Notify the user which signal was caught. We use printf, because this is the
  // most basic output function. Once you get a crash, it is possible that more
  // complex output systems like streams and the like may be corrupted. So we
  // make the most basic call possible to the lowest level, most
  // standard print function.
  if (name) {
    LOG_ERROR("Caught signal %d (%s)", signum, name);
  } else {
    LOG_ERROR("Caught signal %d", signum);
  }

  // Dump a stack trace.
  // This is the function we will be implementing next.
  PrintStackTrace();

  // If you caught one of the above signals, it is likely you just
  // want to quit your program right now.
  exit(signum);
}

void PrintStackTrace() {
  unw_cursor_t cursor;
  unw_context_t context;

  // Initialize cursor to current frame for local unwinding.
  unw_getcontext(&context);
  unw_init_local(&cursor, &context);

  std::printf("\nStackTrace::\n\n");

  // Unwind frames one by one, going up the frame stack.
  while (unw_step(&cursor) > 0) {
    unw_word_t offset, pc;
    unw_get_reg(&cursor, UNW_REG_IP, &pc);
    if (pc == 0) {
      break;
    }
    std::printf("0x%-16lx  ::  ", pc);

    char sym[256];
    if (unw_get_proc_name(&cursor, sym, sizeof(sym), &offset) == 0) {
      char* nameptr = sym;
      int status;
      char* demangled = abi::__cxa_demangle(sym, nullptr, nullptr, &status);
      if (status == 0) {
        nameptr = demangled;
      }
      std::printf(" (%s+0x%lx)\n", nameptr, offset);
      std::free(demangled);
    } else {
      std::printf(" -- error: unable to obtain symbol name for this frame\n");
    }
  }

  std::printf("\n\n");
}


void RegisterSignalHandlers() {
  signal(SIGABRT, SignalHandler);
  signal(SIGSEGV, SignalHandler);
  signal(SIGILL, SignalHandler);
  signal(SIGFPE, SignalHandler);
}

}  // End peloton namespace
