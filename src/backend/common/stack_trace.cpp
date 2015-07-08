/*-------------------------------------------------------------------------
 *
 * stack_trace.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/stack_trace.cpp
 *
 *-------------------------------------------------------------------------
 */

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

#include "backend/common/stack_trace.h"
#include "backend/common/logger.h"

namespace peloton {

// Based on :: http://panthema.net/2008/0901-stacktrace-demangled/
void GetStackTrace(){
  std::stringstream stack_trace;
  std::stringstream internal_info;
  unsigned int max_frames = 63;

  /// storage array for stack trace address data
  void* addrlist[max_frames+1];

  /// retrieve current stack addresses
  int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

  if (addrlen == 0) {
    stack_trace << "<empty, possibly corrupt>\n";
    return;
  }

  /// resolve addresses into strings containing "filename(function+address)",
  /// this array must be free()-ed
  char** symbol_list = backtrace_symbols(addrlist, addrlen);

  /// allocate string which will be filled with the demangled function name
  size_t func_name_size = 4096;
  char* func_name = (char*) malloc(func_name_size);

  /// iterate over the returned symbol lines. skip the first, it is the
  /// address of this function.
  for (int i = 1; i < addrlen; i++){
    char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

    /// find parentheses and +address offset surrounding the mangled name:
    /// ./module(function+0x15c) [0x8048a6d]
    for (char *p = symbol_list[i]; *p; ++p){
      if (*p == '(')
        begin_name = p;
      else if (*p == '+')
        begin_offset = p;
      else if (*p == ')' && begin_offset) {
        end_offset = p;
        break;
      }
    }

    if (begin_name && begin_offset && end_offset && begin_name < begin_offset){
      *begin_name++ = '\0';
      *begin_offset++ = '\0';
      *end_offset = '\0';

      /// mangled name is now in [begin_name, begin_offset) and caller
      /// offset in [begin_offset, end_offset). now apply  __cxa_demangle():
      int status;
      char* ret = abi::__cxa_demangle(begin_name, func_name, &func_name_size, &status);
      if (status == 0) {
        func_name = ret; // use possibly realloc()-ed string
        stack_trace << std::left << std::setw(15) << addrlist[i] << " :: "
            << symbol_list[i] << " [ "
            << func_name << " "
            << begin_offset << " ]\n";
      }
      else {
        /// demangling failed. Output function name as a C function with
        /// no arguments.
        stack_trace << std::left << std::setw(15) << addrlist[i] << " :: "
            << symbol_list[i] << " [ "
            << begin_name << " "
            << begin_offset << " ]\n";
      }
    }
    else {
      /// couldn't parse the line ? print the whole line.
      stack_trace << std::left << std::setw(15) << addrlist[i] << " :: "
          << std::setw(30) << symbol_list[i] << "\n";
    }
  }

  internal_info << "process : "  << getpid() << " thread : " << std::this_thread::get_id();

  LOG_INFO("segmentation fault");
  LOG_INFO("%s", internal_info.str().c_str());
  LOG_INFO("stack trace :\n");
  LOG_INFO("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
  LOG_INFO("%s", stack_trace.str().c_str());
  LOG_INFO("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

  free(func_name);
  free(symbol_list);
}


} // namespace peloton
