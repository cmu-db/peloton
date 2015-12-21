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

namespace peloton {
namespace storage {

#define PMEM_FILE "/mnt/pmfs/peloton.pmem"
#define PMEM_LEN  1024 * 1024 * 1024 * UINT64_C(4) // 4 GB

// global singleton
StorageManager& StorageManager::GetInstance(void) {
  static StorageManager storage_manager;
  return storage_manager;
}

StorageManager::StorageManager()
: pmem_address(nullptr) {
  int pmem_fd;

  // create a pmem file
  if ((pmem_fd = open(PMEM_FILE, O_CREAT | O_RDWR, 0666)) < 0) {
    perror(PMEM_FILE);
    exit(EXIT_FAILURE);
  }

  // allocate the pmem file
  if ((errno = posix_fallocate(pmem_fd, 0, PMEM_LEN)) != 0) {
    perror("posix_fallocate");
    exit(EXIT_FAILURE);
  }

  // memory map the pmem file
  if ((pmem_address = reinterpret_cast<char*>(pmem_map(pmem_fd))) == NULL) {
    perror("pmem_map");
    exit(EXIT_FAILURE);
  }

  // true only if the entire range [addr, addr+len) consists of persistent memory
  is_pmem = pmem_is_pmem(pmem_address, PMEM_LEN);

  // close the pmem file -- it will remain mapped
  close(pmem_fd);
}

StorageManager::~StorageManager() {

  // unmap the pmem file
  pmem_unmap(pmem_address, PMEM_LEN);

}

void *StorageManager::Allocate(BackendType type, size_t size) {

  switch(type) {
    case BACKEND_TYPE_MM:
      return ::operator new(size);

    case BACKEND_TYPE_FILE: {
      {
        std::lock_guard<std::mutex> pmem_lock(pmem_mutex);

        void *address = pmem_address;
        // offset by requested size
        pmem_address += size;
        return address;
      }
    }

    case BACKEND_TYPE_INVALID:
    default:
      return nullptr;
  }

}

void StorageManager::Release(BackendType type, void *address) {

  switch(type) {
    case BACKEND_TYPE_MM:
      ::operator delete(address);
      break;

    case BACKEND_TYPE_FILE:
      // Nothing to do here
      break;

    case BACKEND_TYPE_INVALID:
    default:
      // Nothing to do here
      break;
  }

}

void StorageManager::Sync(BackendType type, void *address, size_t length) {

  switch(type) {
    case BACKEND_TYPE_MM:
      // Nothing to do here
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
    default:
      // Nothing to do here
      break;
  }

}

}  // End storage namespace
}  // End peloton namespace
