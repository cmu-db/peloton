//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logging_util.cpp
//
// Identification: src/logging/logging_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <dirent.h>
#include <cstring>

#include "catalog/manager.h"
#include "logging/logging_util.h"
#include "storage/database.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//

bool LoggingUtil::CreateDirectory(const char *dir_name, int mode) {
  int return_val = mkdir(dir_name, mode);
  if (return_val == 0) {
    LOG_TRACE("Created directory %s successfully", dir_name);
  } else if (errno == EEXIST) {
    LOG_ERROR("Directory %s already exists", dir_name);
  } else {
    LOG_ERROR("Creating directory failed: %s", strerror(errno));
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
    LOG_ERROR("Failed to open directory: %s, error: %s", dir_name,
              strerror(errno));
    return false;
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

void LoggingUtil::FFlushFsync(FileHandle &file_handle) {
  // First, flush
  PELOTON_ASSERT(file_handle.fd != INVALID_FILE_DESCRIPTOR);
  if (file_handle.fd == INVALID_FILE_DESCRIPTOR) return;
  int ret = fflush(file_handle.file);
  if (ret != 0) {
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }
  // Finally, sync
  ret = fsync(file_handle.fd);
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
}

bool LoggingUtil::GetDirectoryList(const char *dir_name,
                                   std::vector<std::string> &dir_name_list) {
  DIR *dir;
  struct dirent *element;

  // get a list of elements in the directory
  dir = opendir(dir_name);
  if (dir == nullptr) {
    LOG_ERROR("Failed to open directory: %s, error: %s", dir_name,
              strerror(errno));
    return false;
  }

  // read directories containing in the directory
  while ((element = readdir(dir)) != nullptr) {
    char *element_name = element->d_name;

    // ignore '.' and '..'
    if (strcmp(element_name, ".") == 0 ||
    		strcmp(element_name, "..") == 0) {
      continue;
    }

      // check directory or not
    std::string target_dir = std::string(dir_name) + '/' + element_name;
    if (CheckDirectoryExistence(target_dir.c_str()) == true) {
    	dir_name_list.push_back(element_name);
    }
  }

  closedir(dir);
  return true;
}

bool LoggingUtil::GetFileList(const char *dir_name,
                              std::vector<std::string> &file_name_list) {
  DIR *dir;
  struct dirent *element;

  // get a list of elements in the directory
  dir = opendir(dir_name);
  if (dir == nullptr) {
    LOG_ERROR("Failed to open directory: %s, error: %s", dir_name,
              strerror(errno));
    return false;
  }

  // read directories containing in the directory
  while ((element = readdir(dir)) != nullptr) {
    char *element_name = element->d_name;

    // ignore '.' and '..'
    if (strcmp(element_name, ".") == 0 ||
    		strcmp(element_name, "..") == 0) {
      continue;
    }

		std::string target_dir = std::string(dir_name) + '/' + element_name;
		if (CheckDirectoryExistence(target_dir.c_str()) != false) {
			file_name_list.push_back(element_name);
		}
  }

  closedir(dir);
  return true;
}

bool LoggingUtil::OpenFile(const char *name, const char *mode,
                           FileHandle &file_handle) {
  auto file = fopen(name, mode);
  if (file == NULL) {
    LOG_ERROR("Checkpoint File is NULL");
    return false;
  } else {
    file_handle.file = file;
  }

  // also, get the descriptor
  auto fd = fileno(file);
  if (fd == INVALID_FILE_DESCRIPTOR) {
    LOG_ERROR("checkpoint_file_fd_ is -1");
    return false;
  } else {
    file_handle.fd = fd;
  }

  file_handle.size = GetFileSize(file_handle);
  return true;
}

bool LoggingUtil::MoveFile(const char *oldname, const char *newname) {
  int ret;
  ret = rename(oldname, newname);
  if (ret != 0) {
    LOG_ERROR("Error occured in rename");
    return false;
  }
  return true;
}

bool LoggingUtil::CloseFile(FileHandle &file_handle) {
  PELOTON_ASSERT(file_handle.file != nullptr &&
                 file_handle.fd != INVALID_FILE_DESCRIPTOR);
  int ret = fclose(file_handle.file);

  if (ret == 0) {
    file_handle.file = nullptr;
    file_handle.fd = INVALID_FILE_DESCRIPTOR;
  } else {
    LOG_ERROR("Error when closing log file");
  }

  return ret == 0;
}

bool LoggingUtil::IsFileTruncated(FileHandle &file_handle,
                                  size_t size_to_read) {
  // Cache current position
  size_t current_position = ftell(file_handle.file);

  // Check if the actual file size is less than the expected file size
  // Current position + frame length
  if (current_position + size_to_read <= file_handle.size) {
    return false;
  } else {
    fseek(file_handle.file, 0, SEEK_END);
    return true;
  }
}

size_t LoggingUtil::GetFileSize(FileHandle &file_handle) {
  struct stat file_stats;
  fstat(file_handle.fd, &file_stats);
  return file_stats.st_size;
}

bool LoggingUtil::ReadNBytesFromFile(FileHandle &file_handle, void *bytes_read,
                                     size_t n) {
  PELOTON_ASSERT(file_handle.fd != INVALID_FILE_DESCRIPTOR &&
                 file_handle.file != nullptr);
  int res = fread(bytes_read, n, 1, file_handle.file);
  return res == 1;
}

}  // namespace logging
}  // namespace peloton
