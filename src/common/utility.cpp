//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// utility.cpp
//
// Identification: src/include/common/utility.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include <vector>
#include <string.h>
#include <errno.h>

#include "common/logger.h"
#include "common/utility.h"
#if __APPLE__
extern "C"{
#include <sys/cdefs.h>
int close$NOCANCEL(int);
};
#endif

namespace peloton {

int peloton_close(int fd) {
  //On Mac OS, close$NOCANCEL guarantees that no descriptor leak & no need to retry on failure.
  //On linux, close will do the same.
  //In short, call close/close$NOCANCEL once and consider it done. AND NEVER RETRY ON FAILURE.
  //The errno code is just a hint. It's logged but no further processing on it.
  //Retry on failure may close another file descriptor that just has been assigned by OS with the same number
  //and break assumptions of other threads.

  int close_ret = -1;
#if __APPLE__
  close_ret = ::close$NOCANCEL(fd);
#else
  close_ret = close(fd);
#endif

  if (close_ret != 0) {
    auto error_message = peloton_error_message();
    LOG_DEBUG("Close failed on fd: %d, errno: %d [%s]", fd, errno, error_message.c_str());
  }

  return close_ret;
}

std::string peloton_error_message() {
  std::vector<char> buffer(100, '\0');
  int saved_errno = errno;
  char *error_message = nullptr;
#if __APPLE__
  (void)strerror_r(errno, buffer.data(), buffer.size() - 1);
  error_message = buffer.data();
#else
  error_message = strerror_r(saved_errno, buffer.data(), buffer.size() - 1);
#endif

  errno = saved_errno;
  return std::string(error_message);
}
}