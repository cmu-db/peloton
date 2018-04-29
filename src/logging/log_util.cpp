/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logging_util.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <dirent.h>
#include <cstring>
#include <unistd.h>

#include "catalog/manager.h"
#include "logging/log_util.h"

namespace peloton {
    namespace logging {

//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//

bool LoggingUtil::CreateDirectory(const char *dir_name, int mode) {
  int return_val = mkdir(dir_name, mode);
  if (return_val == 0) {
    LOG_DEBUG("Created directory %s successfully", dir_name);
  } else if (errno == EEXIST) {
    LOG_DEBUG("Directory %s already exists", dir_name);
  } else {
    LOG_DEBUG("Creating directory failed: %s", strerror(errno));
    return false;
  }
  return true;
}

bool LoggingUtil::CheckDirectoryExistence(const char *dir_name) {
  struct stat info;
  int return_val = stat(dir_name, &info);
  return return_val == 0 && S_ISDIR(info.st_mode);
}

/**
 * @return false if fail to remove directory
 */
bool LoggingUtil::RemoveDirectory(const char *dir_name, bool only_remove_file) {
  struct dirent *file;
  DIR *dir;

  dir = opendir(dir_name);
  if (dir == nullptr) {
    return true;
  }

  // XXX readdir is not thread safe???
  while ((file = readdir(dir)) != nullptr) {
    if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
      continue;
    }
    char complete_path[256];
    strcpy(complete_path, dir_name);
    strcat(complete_path, "/");
    strcat(complete_path, file->d_name);
    auto ret_val = remove(complete_path);
    if (ret_val != 0) {
      LOG_ERROR("Failed to delete file: %s, error: %s", complete_path,
                strerror(errno));
    }
  }
  closedir(dir);
  if (!only_remove_file) {
    auto ret_val = remove(dir_name);
    if (ret_val != 0) {
      LOG_ERROR("Failed to delete dir: %s, error: %s", file->d_name,
                strerror(errno));
    }
  }
  return true;
}

/**
 * @return false if fail to open file
 */
bool LoggingUtil::OpenFile(const char *name, std::ios_base::openmode mode,
                           std::fstream &fs) {
  if(fs.is_open()){
    LOG_ERROR("Failed to open stream: already open");
    return false;
  }

  fs.open(name, mode);

  if (fs.fail()){
    LOG_ERROR("Failed to open stream: %s", strerror(errno));
    return false;
  }

  return true;
}


void LoggingUtil::CloseFile(std::fstream &fs) {
  fs.close();
}

/**
 * @return the number of bytes read from the file
 */
int32_t LoggingUtil::ReadNBytesFromFile(std::fstream &fs, char *bytes_read,
                                     size_t n) {

  PL_ASSERT(!fs.fail());
  fs.read(bytes_read, n);
  return (int32_t)fs.gcount();
}


}  // namespace logging
}  // namespace peloton
