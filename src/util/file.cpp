//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// file.cpp
//
// Identification: src/util/file.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "util/file.h"

#include "util/string_util.h"

namespace peloton {
namespace util {

void File::Open(const std::string &name, File::AccessMode access_mode) {
  // Close the existing file if it's open
  Close();

  int flags;
  switch (access_mode) {
    case AccessMode::ReadOnly: {
      flags = O_RDWR;
      break;
    }
    case AccessMode::WriteOnly: {
      flags = O_WRONLY;
      break;
    }
    case AccessMode::ReadWrite: {
      flags = O_RDWR;
      break;
    }
  }

  // Open
  int fd = open(name.c_str(), flags);

  // Check error
  if (fd == -1) {
    throw Exception(
        StringUtil::Format("unable to read file '%s'", name.c_str()));
  }

  // Done
  fd_ = fd;
}

uint64_t File::Read(void *data, uint64_t len) const {
  // Ensure open
  PELOTON_ASSERT(IsOpen());

  // Perform read
  ssize_t bytes_read = read(fd_, data, len);

  // Check error
  if (bytes_read == -1) {
    throw Exception(
        StringUtil::Format("error reading file: %s", strerror(errno)));
  }

  // Done
  return static_cast<uint64_t>(bytes_read);
}

uint64_t File::Write(void *data, uint64_t len) const {
  // Ensure open
  PELOTON_ASSERT(IsOpen());

  // Perform write
  ssize_t bytes_written = write(fd_, data, len);

  // Check error
  if (bytes_written == -1) {
    throw Exception(
        StringUtil::Format("error writing to file: %s", strerror(errno)));
  }

  // Done
  return static_cast<uint64_t>(bytes_written);
}

uint64_t File::Size() const {
  // Ensure open
  PELOTON_ASSERT(IsOpen());

  // Save the current position
  off_t curr_off = lseek(fd_, 0, SEEK_CUR);
  if (curr_off == -1) {
    throw Exception(StringUtil::Format(
        "unable to read current position in file: %s", strerror(errno)));
  }

  // Seek to the end of the file, returning the new file position i.e., the
  // size of the file in bytes.
  off_t off = lseek(fd_, 0, SEEK_END);
  if (off == -1) {
    throw Exception(StringUtil::Format(
        "unable to move file position to end file: %s", strerror(errno)));
  }

  off_t restore = lseek(fd_, curr_off, SEEK_SET);
  if (restore == -1) {
    throw Exception(StringUtil::Format(
        "unable to restore position after moving to the end: %s",
        strerror(errno)));
  }

  // Restore position
  return static_cast<uint64_t>(off);
}

void File::Close() {
  if (IsOpen()) {
    close(fd_);
    fd_ = kInvalid;
  }
}

}  // namespace util
}  // namespace peloton