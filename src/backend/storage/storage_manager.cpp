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
size_t peloton_pmem_file_size = 0;

namespace peloton {
namespace storage {

#define PMEM_DIR "/mnt/pmfs/"
#define TMP_DIR  "/tmp/"

#define PMEM_LEN  1024 * 1024 * UINT64_C(512) // 512 MB
#define PMEM_FILENAME "peloton.pmem"

// global singleton
StorageManager& StorageManager::GetInstance(void) {
  static StorageManager storage_manager;
  return storage_manager;
}

StorageManager::StorageManager()
: pmem_address(nullptr),
  is_pmem(false),
  pmem_len(0),
  pmem_offset(0) {

  // Check if we need a PMEM pool
  if(peloton_logging_mode != LOGGING_TYPE_PELOTON)
    return;

  int pmem_fd;
  std::string pmem_file_name;
  struct stat pmfs_fd;

  // Initialize pmem file size
  if(peloton_pmem_file_size != 0)
    pmem_len = peloton_pmem_file_size * 1024 * 1024; // MB
  else
    pmem_len = PMEM_LEN;

  // check for pmfs ?
  int status = stat(PMEM_DIR, &pmfs_fd);
  if (status == 0 && S_ISDIR(pmfs_fd.st_mode)) {
    pmem_file_name = std::string(PMEM_DIR) + std::string(PMEM_FILENAME);
  }
  else {
    pmem_file_name = std::string(TMP_DIR) + std::string(PMEM_FILENAME);
  }

  // create a pmem file
  if ((pmem_fd = open(pmem_file_name.c_str(), O_CREAT | O_RDWR, 0666)) < 0) {
    perror(pmem_file_name.c_str());
    exit(EXIT_FAILURE);
  }

  // allocate the pmem file
  if ((errno = posix_fallocate(pmem_fd, 0, pmem_len)) != 0) {
    perror("posix_fallocate");
    exit(EXIT_FAILURE);
  }

  // memory map the pmem file
  if ((pmem_address = reinterpret_cast<char*>(pmem_map(pmem_fd))) == NULL) {
    perror("pmem_map");
    exit(EXIT_FAILURE);
  }

  // true only if the entire range [addr, addr+len) consists of persistent memory
  is_pmem = pmem_is_pmem(pmem_address, pmem_len);

  // close the pmem file -- it will remain mapped
  close(pmem_fd);


}

StorageManager::~StorageManager() {

  // Check if we need a PMEM pool
  if(peloton_logging_mode != LOGGING_TYPE_PELOTON)
    return;

  // unmap the pmem file
  pmem_unmap(pmem_address, pmem_len);

}

void *StorageManager::Allocate(BackendType type, size_t size) {

  switch(type) {
    case BACKEND_TYPE_MM: {
      return ::operator new(size);
    }
    break;

    case BACKEND_TYPE_FILE: {
      {
        std::lock_guard<std::mutex> pmem_lock(pmem_mutex);

        if(pmem_offset >= pmem_len)
          return nullptr;

        void *address = pmem_address + pmem_offset;
        // offset by requested size
        pmem_offset += size;
        return address;
      }
    }
    break;

    case BACKEND_TYPE_INVALID:
    default: {
      return nullptr;
    }
  }

}

void StorageManager::Release(BackendType type, void *address) {

  switch(type) {
    case BACKEND_TYPE_MM: {
      ::operator delete(address);
    }
    break;

    case BACKEND_TYPE_FILE: {
      // Nothing to do here
    }
    break;

    case BACKEND_TYPE_INVALID:
    default: {
      // Nothing to do here
      break;
    }

  }

}

void StorageManager::Sync(BackendType type, void *address, size_t length) {

  switch(type) {
    case BACKEND_TYPE_MM: {
      // Nothing to do here
    }
    break;

    case BACKEND_TYPE_FILE: {
      // flush writes for persistence
      if (is_pmem)
        pmem_persist(address, length);
      else
        pmem_msync(address, length);
    }
    break;

    case BACKEND_TYPE_INVALID:
    default: {
      // Nothing to do here
    }
    break;

  }

}

}  // End storage namespace
}  // End peloton namespace
