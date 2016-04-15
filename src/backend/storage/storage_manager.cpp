//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager.cpp
//
// Identification: src/backend/storage/storage_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>

#include <string>
#include <iostream>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/common/exception.h"
#include "backend/storage/storage_manager.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern LoggingType peloton_logging_mode;

// PMEM file size
size_t peloton_data_file_size = 0;

namespace peloton {
namespace storage {

// 64B cache line size
#define ALIGN 64

static inline void pmem_flush_cache(void *addr, size_t len) {
  uintptr_t uptr = (uintptr_t) addr & ~(ALIGN - 1);
  uintptr_t end = (uintptr_t) addr + len;

  // loop through 64B-aligned chunks covering the given range
  for (; uptr < end; uptr += ALIGN) {
    __builtin_ia32_clflush((void *) uptr);
  }
}

#define DATA_FILE_LEN 1024 * 1024 * UINT64_C(512)  // 512 MB
#define DATA_FILE_NAME "peloton.pmem"

// global singleton
StorageManager &StorageManager::GetInstance(void) {
  static StorageManager storage_manager;
  return storage_manager;
}

StorageManager::StorageManager()
: data_file_address(nullptr),
  data_file_len(0),
  data_file_offset(0) {
  // Check if we need a data pool
  if (IsBasedOnWriteAheadLogging(peloton_logging_mode) == true ||
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
    // Check for NVM FS for data
    case LOGGING_TYPE_NVM_NVM:
    case LOGGING_TYPE_NVM_HDD: {
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

    default:
      break;
  }

  // Fallback to tmp directory if needed
  if (found_file_system == false) {
    int status = stat(TMP_DIR, &data_stat);
    if (status == 0 && S_ISDIR(data_stat.st_mode)) {
      data_file_name = std::string(TMP_DIR) + std::string(DATA_FILE_NAME);
    } else {
      throw Exception("Could not find temp directory : " +
                      std::string(TMP_DIR));
    }
  }

  // TODO:
  LOG_TRACE("DATA DIR :: %s ", data_file_name.c_str());

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

  // map the data file in memory
  if ((data_file_address =  mmap(NULL,
                                 data_file_len,
                                 PROT_READ | PROT_WRITE,
                                 MAP_SHARED,
                                 data_fd, 0)) == MAP_FAILED) {
    perror("mmap");
    exit(EXIT_FAILURE);
  }

  // close the pmem file -- it will remain mapped
  close(data_fd);
}

StorageManager::~StorageManager() {
  // Check if we need a PMEM pool
  if (peloton_logging_mode != LOGGING_TYPE_NVM_NVM) return;

  // sync and unmap the data file
  if(data_file_address != nullptr) {
    // sync the mmap'ed file to SSD or HDD
    int status = msync(data_file_address, data_file_len, MS_SYNC);
    if(status != 0) {
      perror("msync");
      exit(EXIT_FAILURE);
    }

    if (munmap(data_file_address, data_file_len) == MAP_FAILED) {
      perror("munmap");
      exit(EXIT_FAILURE);
    }
  }

}

void *StorageManager::Allocate(BackendType type, size_t size) {
  switch (type) {
    case BACKEND_TYPE_MM: {
      return ::operator new(size);
    } break;

    case BACKEND_TYPE_NVM:
    case BACKEND_TYPE_SSD:
    case BACKEND_TYPE_HDD: {
      {
        std::lock_guard<std::mutex> pmem_lock(pmem_mutex);

        if (data_file_offset >= data_file_len) return nullptr;

        void *address = reinterpret_cast<char*>(data_file_address) + data_file_offset;

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

    case BACKEND_TYPE_NVM:
    case BACKEND_TYPE_SSD:
    case BACKEND_TYPE_HDD: {
      // Nothing to do here
    } break;

    case BACKEND_TYPE_INVALID:
    default: {
      // Nothing to do here
      break;
    }
  }
}

void StorageManager::Sync(BackendType type,
                          void *address,
                          size_t length) {
  switch (type) {
    case BACKEND_TYPE_MM: {
      // Nothing to do here
    } break;

    case BACKEND_TYPE_NVM: {
      // flush writes to NVM
      pmem_flush_cache(address, length);
      __builtin_ia32_sfence();
      clflush_count++;
    } break;

    case BACKEND_TYPE_SSD:
    case BACKEND_TYPE_HDD: {
      // sync the mmap'ed file to SSD or HDD
      int status = msync(data_file_address, data_file_len, MS_SYNC);
      if(status != 0) {
        perror("msync");
        exit(EXIT_FAILURE);
      }

      msync_count++;
    } break;

    case BACKEND_TYPE_INVALID:
    default: {
      // Nothing to do here
    } break;
  }
}

}  // End storage namespace
}  // End peloton namespace
