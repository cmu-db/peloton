//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stack_trace.cpp
//
// Identification: src/backend/common/stack_trace.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <stdexcept>
#include <execinfo.h>
#include <errno.h>
#include <cxxabi.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <sstream>
#include <thread>
#include <iomanip>
#include <string.h>
#include <signal.h>

#include "backend/common/stack_trace.h"

#include "postgres.h"
#include "utils/elog.h"

namespace peloton {

// Based on :: http://panthema.net/2008/0901-stacktrace-demangled/
void GetStackTrace(int signum) {
  std::stringstream stack_trace;
  std::stringstream internal_info;
  unsigned int max_frames = 63;

  /// storage array for stack trace address data
  void *addrlist[max_frames + 1];

  /// retrieve current stack addresses
  int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void *));

  if (addrlen == 0) {
    stack_trace << "<empty, possibly corrupt>\n";
    return;
  }

  /// resolve addresses into strings containing "filename(function+address)",
  /// this array must be free()-ed
  char **symbol_list = backtrace_symbols(addrlist, addrlen);

  /// allocate string which will be filled with the demangled function name
  size_t func_name_size = 4096;
  char *func_name = (char *)malloc(func_name_size);

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

    if (begin_name && begin_offset && end_offset && begin_name < begin_offset) {
      *begin_name++ = '\0';
      *begin_offset++ = '\0';
      *end_offset = '\0';

      /// mangled name is now in [begin_name, begin_offset) and caller
      /// offset in [begin_offset, end_offset). now apply  __cxa_demangle():
      int status;
      char *ret =
          abi::__cxa_demangle(begin_name, func_name, &func_name_size, &status);
      if (status == 0) {
        func_name = ret;  // use possibly realloc()-ed string
        stack_trace << std::left << std::setw(15) << addrlist[i]
                    << " :: " << symbol_list[i] << " [ " << func_name << " "
                    << begin_offset << " ]\n";
      } else {
        /// demangling failed. Output function name as a C function with
        /// no arguments.
        stack_trace << std::left << std::setw(15) << addrlist[i]
                    << " :: " << symbol_list[i] << " [ " << begin_name << " "
                    << begin_offset << " ]\n";
      }
    } else {
      /// couldn't parse the line ? print the whole line.
      stack_trace << std::left << std::setw(15) << addrlist[i]
                  << " :: " << std::setw(30) << symbol_list[i] << "\n";
    }
  }

  internal_info << "process : " << getpid()
                << " thread : " << std::this_thread::get_id();

  elog(LOG, "signal : %s", strsignal(signum));
  elog(LOG, "%s", internal_info.str().c_str());
  elog(LOG, "stack trace :\n");
  elog(
      LOG,
      "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
      "+\n");
  elog(LOG, "\n%s", stack_trace.str().c_str());
  elog(
      LOG,
      "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
      "+\n");

  free(func_name);
  free(symbol_list);
}

// Based on :: https://github.com/bombela/backward-cpp/blob/master/backward.hpp
std::vector<int> StackTracer::make_default_signals() {
  const int signals[] = {
      // default action: Core
      SIGABRT, SIGSEGV,
  };
  return std::vector<int>(signals,
                          signals + sizeof signals / sizeof signals[0]);
}

StackTracer::StackTracer(const std::vector<int> &signals) : _loaded(false) {
  bool success = true;

  const size_t stack_size = 1024 * 1024 * 8;
  _stack_content.reset((char *)malloc(stack_size));
  if (_stack_content) {
    stack_t ss;
    ss.ss_sp = _stack_content.get();
    ss.ss_size = stack_size;
    ss.ss_flags = 0;
    if (sigaltstack(&ss, 0) < 0) {
      success = false;
    }
  } else {
    success = false;
  }

  for (size_t i = 0; i < signals.size(); ++i) {
    struct sigaction action;
    memset(&action, 0, sizeof action);
    action.sa_flags = (SA_SIGINFO | SA_ONSTACK | SA_NODEFER | SA_RESETHAND);
    sigfillset(&action.sa_mask);
    sigdelset(&action.sa_mask, signals[i]);
    action.sa_sigaction = &sig_handler;

    int r = sigaction(signals[i], &action, 0);
    if (r < 0) success = false;
  }

  _loaded = success;
}

void StackTracer::sig_handler(int, siginfo_t *info, void *_ctx) {
  ucontext_t *uctx = (ucontext_t *)_ctx;

  backward::StackTrace st;
  void *error_addr = 0;
#ifdef REG_RIP  // x86_64
  error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.gregs[REG_RIP]);
#elif defined(REG_EIP)  // x86_32
  error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.gregs[REG_EIP]);
#else
#warning ":/ sorry, ain't know no nothing none not of your architecture!"
#endif
  if (error_addr) {
    st.load_from(error_addr, 32);
  } else {
    st.load_here(32);
  }

  backward::Printer printer;
  printer.address = true;
  printer.print(st, stderr);

  psiginfo(info, 0);

  // try to forward the signal.
  raise(info->si_signo);

  // terminate the process immediately.
  puts("watf? exit");
  _exit(EXIT_FAILURE);
}

}  // namespace peloton
