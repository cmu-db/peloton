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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>

#ifdef NVML
#include <libpmem.h>
#else
// PMEM LIBRARY FALLBACK PATH
#define PROCMAXLEN 2048 /* maximum expected line length in /proc files */

#define MEGABYTE ((uintptr_t)1 << 20)
#define GIGABYTE ((uintptr_t)1 << 30)

/* library-wide page size */
unsigned long Pagesize;

static int Mmap_no_random;
static void *Mmap_hint;

#define powerof2(x)     ((((x) - 1) & (x)) == 0)

#ifdef __GNUC__
# define roundup(x, y)  (__builtin_constant_p (y) && powerof2 (y)             \
                         ? (((x) + (y) - 1) & ~((y) - 1))                     \
                         : ((((x) + ((y) - 1)) / (y)) * (y)))
#else
# define roundup(x, y)  ((((x) + ((y) - 1)) / (y)) * (y))
#endif
#endif

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

// PMEM Fallback
#ifndef NVML
int peloton_msync(void *addr, size_t len);
void * peloton_pmem_map(int fd);
void* peloton_util_map(int fd, size_t len, int cow);
int pelonton_pmem_unmap(void *addr, size_t len);
int peloton_util_unmap(void *addr, size_t len);
char * peloton_util_map_hint(size_t len);
void peloton_util_init(void);
#endif

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
    case LOGGING_TYPE_NVM_HDD:
    case LOGGING_TYPE_NVM_SSD: {
      int status = stat(NVM_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        data_file_name = std::string(NVM_DIR) + std::string(DATA_FILE_NAME);
        found_file_system = true;
      }

    } break;

    // Check for SSD FS
    case LOGGING_TYPE_SSD_NVM:
    case LOGGING_TYPE_SSD_SSD:
    case LOGGING_TYPE_SSD_HDD: {
      int status = stat(SSD_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        data_file_name = std::string(SSD_DIR) + std::string(DATA_FILE_NAME);
        found_file_system = true;
      }

    } break;

    // Check for HDD FS
    case LOGGING_TYPE_HDD_NVM:
    case LOGGING_TYPE_HDD_SSD:
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
    }
    else {
      throw Exception("Could not find temp directory : " + std::string(TMP_DIR));
    }
  }

  LOG_INFO("DATA DIR :: %s ", data_file_name.c_str());

  // TODO:
  std::cout << "Data path :: " << data_file_name << "\n";

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

#ifdef NVML
  // memory map the data file
  if ((data_file_address = reinterpret_cast<char *>(pmem_map(data_fd))) ==
      NULL) {
    perror("pmem_map");
    exit(EXIT_FAILURE);
  }

  // true only if the entire range [addr, addr+len) consists of persistent
  // memory
  is_pmem = pmem_is_pmem(data_file_address, data_file_len);
#else
  // Init the pmem stuff
  peloton_util_init();

  // memory map the data file
  if ((data_file_address = reinterpret_cast<char *>(peloton_pmem_map(data_fd))) ==
      NULL) {
    perror("pmem_map");
    exit(EXIT_FAILURE);
  }

  is_pmem = false;
#endif

  // close the pmem file -- it will remain mapped
  close(data_fd);
}

StorageManager::~StorageManager() {
  // Check if we need a PMEM pool
  if (peloton_logging_mode != LOGGING_TYPE_NVM_NVM) return;

  // unmap the pmem file
#ifdef NVML
    if(is_pmem == true)
      pmem_unmap(data_file_address, data_file_len);
#else
    pelonton_pmem_unmap(data_file_address, data_file_len);
#endif
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
#ifdef NVML
      if (is_pmem) {
        pmem_persist(address, length);
        clflush_count++;
      }
#else
        peloton_msync(address, length);
        msync_count++;
#endif
    } break;

    case BACKEND_TYPE_INVALID:
    default: {
      // Nothing to do here
    } break;
  }
}

//===--------------------------------------------------------------------===//
// PMEM Library Fallback
//===--------------------------------------------------------------------===//

#ifndef NVML

int peloton_msync(void *addr, size_t len) {
  /*
   * msync requires len to be a multiple of pagesize, so
   * adjust addr and len to represent the full 4k chunks
   * covering the given range.
   */

  /* increase len by the amount we gain when we round addr down */
  len += (uintptr_t)addr & (Pagesize - 1);

  /* round addr down to page boundary */
  uintptr_t uptr = (uintptr_t)addr & ~(Pagesize - 1);

  /*
   * msync accepts addresses aligned to page boundary, so we may sync
   * more and part of it may have been marked as undefined/inaccessible
   * Msyncing such memory is not a bug, so as a workaround temporarily
   * disable error reporting.
   */

  int ret;
  if ((ret = msync((void *)uptr, len, MS_SYNC)) < 0)
    perror("!msync");

  return ret;
}

/*
 * util_init -- initialize the utils
 *
 * This is called from the library initialization code.
 */
void peloton_util_init(void){
  if (Pagesize == 0)
    Pagesize = (unsigned long) sysconf(_SC_PAGESIZE);

  /*
   * For testing, allow overriding the default mmap() hint address.
   * If hint address is defined, it also disables address randomization.
   */
  char *e = getenv("PMEM_MMAP_HINT");
  if (e) {
    char *endp;
    errno = 0;
    unsigned long long val = strtoull(e, &endp, 16);

    if (errno || endp == e) {
      perror("Invalid PMEM_MMAP_HINT");
    } else {
      Mmap_hint = (void *)val;
      Mmap_no_random = 1;
      perror("PMEM_MMAP_HINT");
    }
  }
}

/*
 * pmem_map -- map the entire file for read/write access
 */
void * peloton_pmem_map(int fd){

  struct stat stbuf;
  if (fstat(fd, &stbuf) < 0) {
    perror("!fstat");
    return NULL;
  }
  if (stbuf.st_size < 0) {
    perror("fstat: negative size");
    return NULL;
  }

  void *addr;
  if ((addr = peloton_util_map(fd, (size_t)stbuf.st_size, 0)) == NULL)
    return NULL;    /* util_map() set errno, called LOG */

  return addr;
}

/*
 * util_map_hint_unused -- use /proc to determine a hint address for mmap()
 *
 * This is a helper function for util_map_hint().
 * It opens up /proc/self/maps and looks for the first unused address
 * in the process address space that is:
 * - greater or equal 'minaddr' argument,
 * - large enough to hold range of given length,
 * - aligned to the specified unit.
 *
 * Asking for aligned address like this will allow the DAX code to use large
 * mappings.  It is not an error if mmap() ignores the hint and chooses
 * different address.
 */
char* peloton_util_map_hint_unused(void *minaddr, size_t len, size_t align) {
  FILE *fp;
  if ((fp = fopen("/proc/self/maps", "r")) == NULL) {
    perror("!/proc/self/maps");
    return NULL;
  }

  char line[PROCMAXLEN];  /* for fgets() */
  char *lo = NULL;  /* beginning of current range in maps file */
  char *hi = NULL;  /* end of current range in maps file */
  char *raddr = (char*) minaddr;  /* ignore regions below 'minaddr' */

  if (raddr == NULL)
    raddr += Pagesize;

  raddr = (char *)roundup((uintptr_t)raddr, align);

  while (fgets(line, PROCMAXLEN, fp) != NULL) {
    /* check for range line */
    if (sscanf(line, "%p-%p", &lo, &hi) == 2) {
      if (lo > raddr) {
        if ((uintptr_t)(lo - raddr) >= len) {
          perror("unused region of size %zu "
              "found at %p");
          break;
        } else {
          perror("region is too small: %zu < %zu");
        }
      }

      if (hi > raddr) {
        raddr = (char *)roundup((uintptr_t)hi, align);
        perror("nearest aligned addr %p");
      }

      if (raddr == 0) {
        perror("end of address space reached");
        break;
      }
    }
  }

  /*
   * Check for a case when this is the last unused range in the address
   * space, but is not large enough. (very unlikely)
   */
  if ((raddr != NULL) && (UINTPTR_MAX - (uintptr_t)raddr < len)) {
    perror("end of address space reached");
    raddr = NULL;
  }

  fclose(fp);

  return raddr;
}

/*
 * util_map_hint -- determine hint address for mmap()
 *
 * If PMEM_MMAP_HINT environment variable is not set, we let the system to pick
 * the randomized mapping address.  Otherwise, a user-defined hint address
 * is used.
 *
 * ALSR in 64-bit Linux kernel uses 28-bit of randomness for mmap
 * (bit positions 12-39), which means the base mapping address is randomized
 * within [0..1024GB] range, with 4KB granularity.  Assuming additional
 * 1GB alignment, it results in 1024 possible locations.
 *
 * Configuring the hint address via PMEM_MMAP_HINT environment variable
 * disables address randomization.  In such case, the function will search for
 * the first unused, properly aligned region of given size, above the specified
 * address.
 */
char * peloton_util_map_hint(size_t len) {
  char *addr;

  /*
   * Choose the desired alignment based on the requested length.
   * Use 2MB/1GB page alignment only if the mapping length is at least
   * twice as big as the page size.
   */
  size_t align = Pagesize;
  if (len >= 2 * GIGABYTE)
    align = GIGABYTE;
  else if (len >= 4 * MEGABYTE)
    align = 2 * MEGABYTE;

  if (Mmap_no_random) {
    addr = peloton_util_map_hint_unused((void *)Mmap_hint, len, align);
  } else {
    /*
     * Create dummy mapping to find an unused region of given size.
     * Request for increased size for later address alignment.
     * Use MAP_PRIVATE with read-only access to simulate
     * zero cost for overcommit accounting.  Note: MAP_NORESERVE
     * flag is ignored if overcommit is disabled (mode 2).
     */
    addr = (char*) mmap(NULL, len + align, PROT_READ,
          MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (addr == MAP_FAILED) {
      addr = NULL;
    } else {
      munmap(addr, len + align);
      addr = (char *)roundup((uintptr_t)addr, align);
    }
  }

  return addr;
}

/*
 * util_map -- memory map a file
 *
 * This is just a convenience function that calls mmap() with the
 * appropriate arguments and includes our trace points.
 *
 * If cow is set, the file is mapped copy-on-write.
 */
void* peloton_util_map(int fd, size_t len, int cow) {
  void *base;
  void *addr = peloton_util_map_hint(len);

  if ((base = mmap(addr, len, PROT_READ|PROT_WRITE,
                   (cow) ? MAP_PRIVATE|MAP_NORESERVE : MAP_SHARED,
                       fd, 0)) == MAP_FAILED) {
    perror("!mmap");
    return NULL;
  }

  return base;
}

/*
 * pmem_unmap -- unmap the specified region
 */
int pelonton_pmem_unmap(void *addr, size_t len){
  int ret = peloton_util_unmap(addr, len);

  return ret;
}

/*
 * util_unmap -- unmap a file
 *
 * This is just a convenience function that calls munmap() with the
 * appropriate arguments and includes our trace points.
 */
int peloton_util_unmap(void *addr, size_t len) {
  int retval = munmap(addr, len);
  if (retval < 0)
    perror("!munmap");

  return retval;
}

#endif

}  // End storage namespace
}  // End peloton namespace
