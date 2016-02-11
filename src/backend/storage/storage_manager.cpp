//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// storage_manager.cpp
//
// Identification: src/backend/storage/storage_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/storage/storage_manager.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <libpmem.h>

#include <string>
#include <iostream>

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern LoggingType peloton_logging_mode;

// PMEM file size
size_t peloton_data_file_size = 0;

namespace peloton {
namespace storage {

#define DATA_FILE_LEN 1024 * 1024 * UINT64_C(512)  // 512 MB
#define DATA_FILE_NAME "peloton.pmem"

// global singleton
StorageManager &StorageManager::GetInstance(void) {
  static StorageManager storage_manager;
  return storage_manager;
}

StorageManager::StorageManager()
    : data_file_address(nullptr),
      is_pmem(false),
      data_file_len(0),
      data_file_offset(0) {
  // Check if we need a data pool
  if (IsSimilarToARIES(peloton_logging_mode) == true ||
      peloton_logging_mode == LOGGING_TYPE_INVALID) {
    return;
  }

  int data_fd;
  std::string data_file_name;
  struct stat data_stat;

  // Initialize file size
  if (peloton_data_file_size != 0)
    data_file_len = peloton_data_file_size * 1024 * 1024;  // MB
  else
    data_file_len = DATA_FILE_LEN;

  // Check for relevant file system
  bool found_file_system = false;

  switch (peloton_logging_mode) {
    // Check for NVM FS
    case LOGGING_TYPE_NVM_NVM:
    case LOGGING_TYPE_NVM_HDD:
    case LOGGING_TYPE_NVM_SSD: {
      int status = stat(NVM_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        data_file_name = std::string(NVM_DIR) + std::string(DATA_FILE_NAME);
        found_file_system = true;
      }

    } break;

    // Check for HDD FS
    case LOGGING_TYPE_HDD_NVM:
    case LOGGING_TYPE_HDD_HDD: {
      int status = stat(HDD_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        data_file_name = std::string(HDD_DIR) + std::string(DATA_FILE_NAME);
        found_file_system = true;
      }

    } break;

    // Check for SSD FS
    case LOGGING_TYPE_SSD_NVM:
    case LOGGING_TYPE_SSD_SSD: {
      int status = stat(SSD_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        data_file_name = std::string(SSD_DIR) + std::string(DATA_FILE_NAME);
        found_file_system = true;
      }

    } break;

    default:
      break;
  }

  // Fallback to tmp if needed
  if (found_file_system == false) {
    data_file_name = std::string(TMP_DIR) + std::string(DATA_FILE_NAME);
  }

  LOG_INFO("DATA DIR :: %s ", data_file_name.c_str());

  // Create a data file
  if ((data_fd = open(data_file_name.c_str(), O_CREAT | O_RDWR, 0666)) < 0) {
    perror(data_file_name.c_str());
    exit(EXIT_FAILURE);
  }

  // Allocate the data file
  if ((errno = posix_fallocate(data_fd, 0, data_file_len)) != 0) {
    perror("posix_fallocate");
    exit(EXIT_FAILURE);
  }

  // memory map the data file
  if ((data_file_address = reinterpret_cast<char *>(pmem_map(data_fd))) ==
      NULL) {
    perror("pmem_map");
    exit(EXIT_FAILURE);
  }

  // true only if the entire range [addr, addr+len) consists of persistent
  // memory
  is_pmem = pmem_is_pmem(data_file_address, data_file_len);

  // close the pmem file -- it will remain mapped
  close(data_fd);
}

StorageManager::~StorageManager() {
  // Check if we need a PMEM pool
  if (peloton_logging_mode != LOGGING_TYPE_NVM_NVM) return;

  // unmap the pmem file
  pmem_unmap(data_file_address, data_file_len);
}

void *StorageManager::Allocate(BackendType type, size_t size) {
  switch (type) {
    case BACKEND_TYPE_MM: {
      return ::operator new(size);
    } break;

    case BACKEND_TYPE_FILE: {
      {
        std::lock_guard<std::mutex> pmem_lock(pmem_mutex);

        if (data_file_offset >= data_file_len) return nullptr;

        void *address = data_file_address + data_file_offset;
        // offset by requested size
        data_file_offset += size;
        return address;
      }
    } break;

    case BACKEND_TYPE_INVALID:
    default: { return nullptr; }
  }
}

void StorageManager::Release(BackendType type, void *address) {
  switch (type) {
    case BACKEND_TYPE_MM: {
      ::operator delete(address);
    } break;

    case BACKEND_TYPE_FILE: {
      // Nothing to do here
    } break;

    case BACKEND_TYPE_INVALID:
    default: {
      // Nothing to do here
      break;
    }
  }
}

void StorageManager::Sync(BackendType type, void *address, size_t length) {
  switch (type) {
    case BACKEND_TYPE_MM: {
      // Nothing to do here
    } break;

    case BACKEND_TYPE_FILE: {
      // flush writes for persistence
      if (is_pmem)
        pmem_persist(address, length);
      else
        pmem_msync(address, length);
    } break;

    case BACKEND_TYPE_INVALID:
    default: {
      // Nothing to do here
    } break;
  }
}

}  // End storage namespace
}  // End peloton namespace
