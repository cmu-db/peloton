//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// storage_manager.cpp
//
// Identification: src/storage/storage_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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
#include <cpuid.h>

#include <string>
#include <iostream>

#include "common/types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/exception.h"
#include "storage/storage_manager.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern LoggingType peloton_logging_mode;

// PMEM file size
size_t peloton_data_file_size = 0;

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// INSTRUCTIONS
//===--------------------------------------------------------------------===//

// Source : https://github.com/pmem/nvml/blob/master/src/libpmem/pmem.c

// 64B cache line size
#define FLUSH_ALIGN ((uintptr_t)64)

#define EAX_IDX 0
#define EBX_IDX 1
#define ECX_IDX 2
#define EDX_IDX 3

#ifndef bit_CLFLUSH
#define bit_CLFLUSH (1 << 23)
#endif

#ifndef bit_PCOMMIT
#define bit_PCOMMIT (1 << 22)
#endif

#ifndef bit_CLFLUSHOPT
#define bit_CLFLUSHOPT  (1 << 23)
#endif

#ifndef bit_CLWB
#define bit_CLWB  (1 << 24)
#endif

/*
 * The x86 memory instructions are new enough that the compiler
 * intrinsic functions are not always available.  The intrinsic
 * functions are defined here in terms of asm statements for now.
 */
#define _mm_clflushopt(addr)\
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)addr));
#define _mm_clwb(addr)\
    asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)addr));
#define _mm_pcommit()\
    asm volatile(".byte 0x66, 0x0f, 0xae, 0xf8");

//===--------------------------------------------------------------------===//
// CPU CHECK
//===--------------------------------------------------------------------===//

static inline void cpuid(unsigned func, unsigned subfunc, unsigned cpuinfo[4]){
  __cpuid_count(func, subfunc, cpuinfo[EAX_IDX], cpuinfo[EBX_IDX],
                cpuinfo[ECX_IDX], cpuinfo[EDX_IDX]);
}

/*
 * is_cpu_genuine_intel -- checks for genuine Intel CPU
 */
int is_cpu_genuine_intel(void) {
  unsigned cpuinfo[4] = { 0 };

  union {
    char name[0x20];
    unsigned cpuinfo[3];
  } vendor;

  PL_MEMSET(&vendor, 0, sizeof (vendor));

  cpuid(0x0, 0x0, cpuinfo);

  vendor.cpuinfo[0] = cpuinfo[EBX_IDX];
  vendor.cpuinfo[1] = cpuinfo[EDX_IDX];
  vendor.cpuinfo[2] = cpuinfo[ECX_IDX];

  return (strncmp(vendor.name, "GenuineIntel",
                  sizeof (vendor.name))) == 0;
}

//===--------------------------------------------------------------------===//
// INSTRUCTION CHECKS
//===--------------------------------------------------------------------===//

/*
 * is_cpu_clflush_present -- checks if CLFLUSH instruction is supported
 */
int is_cpu_clflush_present(void){
  unsigned cpuinfo[4] = { 0 };

  cpuid(0x1, 0x0, cpuinfo);

  int ret = (cpuinfo[EDX_IDX] & bit_CLFLUSH) != 0;

  return ret;
}

/*
 * is_cpu_clwb_present -- checks if CLWB instruction is supported
 */
int is_cpu_clwb_present(void){
  unsigned cpuinfo[4] = { 0 };

  if (!is_cpu_genuine_intel())
    return 0;

  cpuid(0x7, 0x0, cpuinfo);

  int ret = (cpuinfo[EBX_IDX] & bit_CLWB) != 0;

  return ret;
}

/*
 * is_cpu_pcommit_present -- checks if PCOMMIT instruction is supported
 */
int is_cpu_pcommit_present(void){
  unsigned cpuinfo[4] = { 0 };

  if (!is_cpu_genuine_intel())
    return 0;

  cpuid(0x7, 0x0, cpuinfo);

  int ret = (cpuinfo[EBX_IDX] & bit_PCOMMIT) != 0;

  return ret;
}

//===--------------------------------------------------------------------===//
// FLUSH FUNCTIONS
//===--------------------------------------------------------------------===//

/*
 * flush_clflush -- (internal) flush the CPU cache, using clflush
 */
static inline void flush_clflush(const void *addr, size_t len){
  uintptr_t uptr;

  // Loop through cache-line-size (typically 64B) aligned chunks
  // covering the given range.
  for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
      uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN)
    _mm_clflush((char *)uptr);
}

// flush_clwb -- (internal) flush the CPU cache, using clwb
static inline void flush_clwb(const void *addr, size_t len) {
  uintptr_t uptr;

  // Loop through cache-line-size (typically 64B) aligned chunks
  // covering the given range.
  for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
      uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN) {
    _mm_clwb((char *)uptr);
  }
}

/*
 * pmem_flush() calls through Func_flush to do the work.  Although
 * initialized to flush_clflush(), once the existence of the clflushopt
 * feature is confirmed by pmem_init() at library initialization time,
 * Func_flush is set to flush_clflushopt().  That's the most common case
 * on modern hardware that supports persistent memory.
 */
static void (*Func_flush)(const void *, size_t) = flush_clflush;

//===--------------------------------------------------------------------===//
// PREDRAIN FUNCTIONS
//===--------------------------------------------------------------------===//

/*
 * predrain_fence_empty -- (internal) issue the pre-drain fence instruction
 */
static void predrain_fence_empty(void) {
  /* nothing to do (because CLFLUSH did it for us) */
}

/*
 * predrain_fence_sfence -- (internal) issue the pre-drain fence instruction
 */
static void predrain_fence_sfence(void) {

  _mm_sfence(); /* ensure CLWB or CLFLUSHOPT completes before PCOMMIT */
}

/*
 * pmem_drain() calls through Func_predrain_fence to do the fence.  Although
 * initialized to predrain_fence_empty(), once the existence of the CLWB or
 * CLFLUSHOPT feature is confirmed by pmem_init() at library initialization
 * time, Func_predrain_fence is set to predrain_fence_sfence().  That's the
 * most common case on modern hardware that supports persistent memory.
 */
static void (*Func_predrain_fence)(void) = predrain_fence_empty;

//===--------------------------------------------------------------------===//
// DRAIN FUNCTIONS
//===--------------------------------------------------------------------===//

/*
 * drain_no_pcommit -- (internal) wait for PM stores to drain, empty version
 */
static void drain_no_pcommit(void){

  Func_predrain_fence();

  /* caller assumed responsibility for the rest */
}

/*
 * drain_pcommit -- (internal) wait for PM stores to drain, pcommit version
 */
static void drain_pcommit(void) {
  Func_predrain_fence();

  _mm_pcommit();
  _mm_sfence();
}

/*
 * pmem_drain() calls through Func_drain to do the work.  Although
 * initialized to drain_no_pcommit(), once the existence of the pcommit
 * feature is confirmed by pmem_init() at library initialization time,
 * Func_drain is set to drain_pcommit().  That's the most common case
 * on modern hardware that supports persistent memory.
 */
static void (*Func_drain)(void) = drain_no_pcommit;

//===--------------------------------------------------------------------===//
// STORAGE MANAGER
//===--------------------------------------------------------------------===//

#define DATA_FILE_LEN 1024 * 1024 * UINT64_C(512)  // 512 MB
#define DATA_FILE_NAME "peloton.pmem"

// global singleton
StorageManager &StorageManager::GetInstance(void) {
  static StorageManager storage_manager;
  return storage_manager;
}

StorageManager::StorageManager()
: data_file_address(nullptr), data_file_len(0), data_file_offset(0) {
  // Check if we need a data pool
  if (IsBasedOnWriteAheadLogging(peloton_logging_mode) == true ||
      peloton_logging_mode == LOGGING_TYPE_INVALID) {
    return;
  }

  // Check for instruction availability
  if(is_cpu_clwb_present()) {
    LOG_TRACE("Found clwb \n");
    Func_flush = flush_clwb;
    Func_predrain_fence = predrain_fence_sfence;
  }

  if (is_cpu_pcommit_present()) {
    LOG_TRACE("Found pcommit \n");
    Func_drain = drain_pcommit;
  }

  // Rest of this stuff is needed only for Write Behind Logging
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
    case LOGGING_TYPE_NVM_WBL: {
      int status = stat(NVM_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        data_file_name = std::string(NVM_DIR) + std::string(DATA_FILE_NAME);
        found_file_system = true;
      }

    } break;

    // Check for SSD FS for data
    case LOGGING_TYPE_SSD_WBL: {
      int status = stat(SSD_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        data_file_name = std::string(SSD_DIR) + std::string(DATA_FILE_NAME);
        found_file_system = true;
      }

    } break;

    // Check for HDD FS
    case LOGGING_TYPE_HDD_WBL: {
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

  LOG_TRACE("DATA DIR :: %s ", data_file_name.c_str());

  // Create a data file
  if ((data_fd = open(data_file_name.c_str(), 
                      O_CREAT | O_TRUNC | O_RDWR,
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) < 0) {
    perror(data_file_name.c_str());
    exit(EXIT_FAILURE);
  }

  // Allocate the data file
  if ((errno = posix_fallocate(data_fd, 0, data_file_len)) != 0) {
    perror("posix_fallocate");
    exit(EXIT_FAILURE);
  }

  // map the data file in memory
  if ((data_file_address = mmap(NULL, data_file_len, PROT_READ | PROT_WRITE,
                                MAP_SHARED, data_fd, 0)) == MAP_FAILED) {
    perror("mmap");
    exit(EXIT_FAILURE);
  }

  // close the pmem file -- it will remain mapped
  close(data_fd);
}

StorageManager::~StorageManager() {

  LOG_TRACE("Allocation count : %ld \n", allocation_count);

  // Check if we need a PMEM pool
  if (peloton_logging_mode != LOGGING_TYPE_NVM_WBL) return;

  // sync and unmap the data file
  if (data_file_address != nullptr) {
    // sync the mmap'ed file to SSD or HDD
    int status = msync(data_file_address, data_file_len, MS_SYNC);
    if (status != 0) {
      perror("msync");
      exit(EXIT_FAILURE);
    }

    if (munmap(data_file_address, data_file_len)) {
      perror("munmap");
      exit(EXIT_FAILURE);
    }
  }

}

void *StorageManager::Allocate(BackendType type, size_t size) {
  // Update allocation count
  allocation_count++;

  switch (type) {
    case BACKEND_TYPE_MM:
    case BACKEND_TYPE_NVM:{
      return ::operator new(size);
    } break;

    case BACKEND_TYPE_SSD:
    case BACKEND_TYPE_HDD: {
      {
        size_t cache_data_file_offset = 0;

        // Lock the file
        data_file_spinlock.Lock();

        // Check if within bounds
        if (data_file_offset < data_file_len) {
          cache_data_file_offset = data_file_offset;

          // Offset by the requested size
          data_file_offset += size;

          // Unlock the file
          data_file_spinlock.Unlock();

          void *address = reinterpret_cast<char*>(data_file_address) + cache_data_file_offset;
          return address;
        }

        data_file_spinlock.Unlock();
        throw Exception("no more memory available: offset : " + std::to_string(data_file_offset) +
                        " length : " + std::to_string(data_file_len));

        return nullptr;
      }
    } break;

    case BACKEND_TYPE_INVALID:
    default: {
      throw Exception("invalid backend: " + std::to_string(data_file_len));
      return nullptr;
    }
  }
}

void StorageManager::Release(BackendType type, void *address) {
  switch (type) {
    case BACKEND_TYPE_MM:
    case BACKEND_TYPE_NVM:{
      ::operator delete(address);
    } break;

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

void StorageManager::Sync(BackendType type, void *address, size_t length) {
  switch (type) {
    case BACKEND_TYPE_MM: {
      // Nothing to do here
    } break;

    case BACKEND_TYPE_NVM: {
      // flush writes to NVM
      Func_flush(address, length);
      Func_drain();
      clflush_count++;
    } break;

    case BACKEND_TYPE_SSD:
    case BACKEND_TYPE_HDD: {
      // sync the mmap'ed file to SSD or HDD
      int status = msync(data_file_address, data_file_len, MS_SYNC);
      if (status != 0) {
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
