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
  // create a big file
  std::ofstream backend_file(file_name, std::ios::binary | std::ios::out);
  backend_file.seekp(file_size - 1);
  backend_file.write("", 1);

  // do mmap
  fd = open(file_name.c_str(), O_RDWR, 0);
  backend_space = (char *)mmap(nullptr, file_size, PROT_READ|PROT_WRITE, MAP_FILE|MAP_SHARED, fd, 0);
  close(fd);
}

BackendFile::~BackendFile() {
  munmap(backend_space, file_size);
  //remove(file_name.c_str());
}

void *BackendFile::Allocate(size_t size) {
  void *ret = nullptr;
  {
    std::lock_guard<std::mutex> lock(backend_mutex);
    ret = (void *) (backend_space + current_pos);
    current_pos += size;
  }
  if (ret) {
    chunk_size_recs.insert(std::make_pair(ret, size));
  }
  return ret;
}

void BackendFile::Free(void *ptr) {
  // do nothing about the space
  if (chunk_size_recs.find(ptr) != chunk_size_recs.end()) {
    chunk_size_recs.erase(ptr);
  }
}

void BackendFile::Sync(void *ptr) {
  if (chunk_size_recs.find(ptr) != chunk_size_recs.end()) {
    msync(ptr, chunk_size_recs.at(ptr), MS_SYNC);
  }
}

}  // End storage namespace
}  // End peloton namespace
