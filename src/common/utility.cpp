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
#include <include/common/logger.h>
#include <vector>
#include <string.h>

#if __APPLE__
extern "C"{
#include <sys/cdefs.h>
int close$NOCANCEL(int);
};
#endif

namespace peloton {


int peloton_close(int fd, int failure_log_level = LOG_LEVEL_DEBUG) {
  int close_ret = -1;
#if __APPLE__
  close_ret = ::close$NOCANCEL(sock_fd_);
#else
  close_ret = close(fd);
#endif

  if (close_ret != 0) {
    std::vector<char> buffer(100, '\0');
    int saved_errno = errno;
    char *error_message = nullptr;
#if __APPLE__
    (void)strerror_r(errno, buffer.data(), buffer.size() - 1);
    error_message = buffer.data();
#else
    error_message = strerror_r(saved_errno, buffer.data(), buffer.size() - 1);
#endif
    (void) error_message;

    const char *format_string = "Close failed on fd: %d, errno: %d [%s]";

    switch (failure_log_level) {
      case LOG_LEVEL_TRACE:
        LOG_TRACE(format_string, fd, saved_errno, error_message);
        break;
      case LOG_LEVEL_DEBUG:
        LOG_DEBUG(format_string, fd, saved_errno, error_message);
        break;
      case LOG_LEVEL_INFO:
        LOG_INFO(format_string, fd, saved_errno, error_message);
        break;
      case LOG_LEVEL_WARN:
        LOG_WARN(format_string, fd, saved_errno, error_message);
        break;
      case LOG_LEVEL_ERROR:
        LOG_ERROR(format_string, fd, saved_errno, error_message);
        break;
    }
  }

  return close_ret;
}
}