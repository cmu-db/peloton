//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_backend.h
//
// Identification: src/backend/storage/backend_file.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/backend_file.h"
#include "backend/logging/log_manager.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <fstream>
#include <unistd.h>

namespace peloton {
namespace storage {

BackendFile::BackendFile() {
  if (EnableBackFileType()) {
    // create a big file
    std::ofstream backend_file(file_name, std::ios::binary | std::ios::out);
    backend_file.seekp(file_size - 1);
    backend_file.write("", 1);
    backend_file.close();

    // do mmap
    fd = open(file_name.c_str(), O_RDWR, 0);
    backend_space = mmap(nullptr, file_size, PROT_READ | PROT_WRITE,
                                                  MAP_FILE | MAP_SHARED, fd, 0);
    close(fd);
    assert(backend_space != nullptr);
    // create vmem pool
    vmp = vmem_create_in_region(backend_space, file_size);
    assert(vmp != nullptr);
  }
}

BackendFile::~BackendFile() {
  if (vmp != nullptr) {
    vmem_delete(vmp);
  }
  if (backend_space != nullptr) {
    munmap(backend_space, file_size);
    //remove(file_name.c_str());
  }
}

void *BackendFile::Allocate(size_t size) {
  if (backend_space != nullptr) {
    return vmem_malloc(vmp, size);;
  } else {
    return ::operator new(size);
  }
}

void BackendFile::Free(void *ptr) {
  if (backend_space != nullptr) {
    vmem_free(vmp, ptr);
  } else {
    ::operator delete(ptr);
  }
}

void BackendFile::Sync(void *ptr) {
  if (backend_space != nullptr) {
    size_t ptr_size = vmem_malloc_usable_size(vmp, ptr);
    if (ptr_size != 0) {
      msync(ptr, ptr_size, MS_SYNC);
    }
  }
}

}  // End storage namespace
}  // End peloton namespace
