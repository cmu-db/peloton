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


#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <execinfo.h>
#include <errno.h>
#include <cxxabi.h>
#include <signal.h>
#include <memory>

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

// Based on :: http://panthema.net/2008/0901-stacktrace-demangled/
void PrintStackTrace(FILE *out, unsigned int max_frames) {
  ::fprintf(out, "Stack Trace:\n");

  /// storage array for stack trace address data
  void *addrlist[max_frames + 1];

  /// retrieve current stack addresses
  int addrlen = backtrace(addrlist, max_frames);

  if (addrlen == 0) {
    ::fprintf(out, "  <empty, possibly corrupt>\n");
    return;
  }

  /// resolve addresses into strings containing "filename(function+address)",
  char** symbol_list = backtrace_symbols(addrlist, addrlen);

  /// allocate string which will be filled with the demangled function name
  size_t func_name_size = 1024;
  std::unique_ptr<char> func_name(new char[func_name_size]);

  /// iterate over the returned symbol lines. skip the first, it is the
  /// address of this function.
  for (int i = 1; i < addrlen; i++) {
    char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

    /// find parentheses and +address offset surrounding the mangled name:
    /// ./module(function+0x15c) [0x8048a6d]
    for (char *p = symbol_list[i]; *p; ++p) {
      if (*p == '(')
        begin_name = p;
      else if (*p == '+')
        begin_offset = p;
      else if (*p == ')' && begin_offset) {
        end_offset = p;
        break;
      }
    }

    if (begin_name && begin_offset && end_offset &&
        begin_name < begin_offset) {
      *begin_name++ = '\0';
      *begin_offset++ = '\0';
      *end_offset = '\0';

      /// mangled name is now in [begin_name, begin_offset) and caller
      /// offset in [begin_offset, end_offset). now apply  __cxa_demangle():
      int status;
      char *ret = abi::__cxa_demangle(begin_name, func_name.get(), &func_name_size,
                                      &status);
      if (status == 0) {
        func_name.reset(ret);  // use possibly realloc()-ed string
        ::fprintf(out, "  %s : %s+%s\n", symbol_list[i], func_name.get(),
                  begin_offset);
      } else {
        /// demangling failed. Output function name as a C function with
        /// no arguments.
        ::fprintf(out, "  %s : %s()+%s\n", symbol_list[i], begin_name,
                  begin_offset);
      }
    } else {
      /// couldn't parse the line ? print the whole line.
      ::fprintf(out, "  %s\n", symbol_list[i]);
    }
  }

}

void RegisterSignalHandlers() {
  signal(SIGABRT, SignalHandler);
  signal(SIGSEGV, SignalHandler);
  signal(SIGILL, SignalHandler);
  signal(SIGFPE, SignalHandler);
}

}  // End peloton namespace
