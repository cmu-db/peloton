#include <iostream>
#include <fcntl.h>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <getopt.h>
#include <cstdint>

using namespace std;

#define roundup2(x, y)  (((x)+((y)-1))&(~((y)-1))) /* if y is powers of two */

class timer {
 public:

  timer() {
    total = timeval();
  }

  double duration() {
    double duration;

    duration = (total.tv_sec) * 1000.0;      // sec to ms
    duration += (total.tv_usec) / 1000.0;      // us to ms

    return duration;
  }

  void start() {
    gettimeofday(&t1, NULL);
  }

  void end() {
    gettimeofday(&t2, NULL);
    timersub(&t2, &t1, &diff);
    timeradd(&diff, &total, &total);
  }

  void reset() {
    total = timeval();
  }

  timeval t1, t2, diff;
  timeval total;
};

#define ALIGN 64

static inline void pmem_persist(void *addr, size_t len) {
  uintptr_t uptr = (uintptr_t) addr & ~(ALIGN - 1);
  uintptr_t end = (uintptr_t) addr + len;

  // loop through 64B-aligned chunks covering the given range
  for (; uptr < end; uptr += ALIGN) {
    __builtin_ia32_clflush((void *) uptr);
  }

  __builtin_ia32_sfence();
}


#define IO_ALIGN    4096

static void usage_exit(FILE *out) {
  fprintf(
      out,
      "Command line options : nstore <options> \n"
      "   -h --help              :  Print help message \n"
      "   -r --random-mode       :  Random accesses \n"
      "   -c --chunk-size        :  Chunk size \n"
      "   -f --fs-type           :  FS type (0 : NVM, 1: PMFS, 2: EXT4, 3:TMPFS \n");
  exit(EXIT_FAILURE);
}

static struct option opts[] = { { "random-mode", no_argument, NULL, 'r' }, {
    "fs-type", optional_argument, NULL, 'f' }, { "chunk-size",
        optional_argument, NULL, 'c' }, { "help", no_argument, NULL, 'h' }, { NULL, 0,
            NULL, 0 } };

class config {
 public:

  bool random_mode;
  bool sync_mode;
  bool nvm_mode;
  int fs_type;
  int chunk_size;

  std::string fs_path;
};

static void parse_arguments(int argc, char* argv[], config& state) {

  // Default Values
  state.fs_type = 1;
  state.fs_path = "/mnt/pmfs/";

  state.random_mode = false;
  state.sync_mode = false;
  state.nvm_mode = false;
  state.chunk_size = 64;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "f:c:hr", opts, &idx);

    if (c == -1)
      break;

    switch (c) {
      case 'f':
        state.fs_type = atoi(optarg);
        break;

      case 'r':
        state.random_mode = true;
        cout << "random_mode " << endl;
        break;

      case 's':
        state.sync_mode = true;
        cout << "sync_mode " << endl;
        break;

      case 'c':
        state.chunk_size = atoi(optarg);
        cout << "chunk_size : " << state.chunk_size << endl;
        break;

      case 'h':
        usage_exit(stderr);
        break;
      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        usage_exit(stderr);
    }
  }

  switch (state.fs_type) {
    case 0:
      state.nvm_mode = true;
      cout << "nvm_mode " << endl;
      break;

    case 1:
      state.fs_path = "/mnt/pmfs/";
      cout << "fs_path : " << state.fs_path << endl;
      break;

    case 2:
      state.fs_path = "./";
      cout << "fs_path : " << state.fs_path << endl;
      break;

    case 3:
      state.fs_path = "/data/";
      cout << "fs_path : " << state.fs_path << endl;
      break;

    default:
      fprintf(stderr, "\nUnknown fs_type : -%d-\n", state.fs_type);
      usage_exit(stderr);
  }

}

int main(int argc, char **argv) {

  config state;
  parse_arguments(argc, argv, state);

  FILE* fp = NULL;
  int fd = -1;
  size_t chunk_size, file_size, offset;
  unsigned int i, j, itr_cnt;
  timer tm;

  file_size = 512 * 1024 * 1024;  // 512 MB

  bool random_mode = state.random_mode;
  bool sync_mode = state.sync_mode;
  bool nvm_mode = state.nvm_mode;
  std::string fs_prefix = state.fs_path;
  chunk_size = state.chunk_size;
  std::string path = state.fs_path;
  double iops;

  itr_cnt = 128 * 8;

  size_t min_chunk_size = 1;
  size_t max_chunk_size = 512;

  char buf[max_chunk_size + 1];
  for (j = 0; j < max_chunk_size; j++)
    buf[j] = ('a' + rand() % 5);

  offset = 0;

  for (int random = 0; random <= 1; random++) {
    random_mode = (bool) random;
    printf("RANDOM \t:\t %d \n", random);

    for (chunk_size = min_chunk_size; chunk_size <= max_chunk_size;
        chunk_size *= 2) {
      printf("%lu ,", chunk_size);

      // NVM MODE
      char* nvm_buf = new char[file_size + chunk_size + 1];

      // WRITE
      for (j = 0; j < itr_cnt; j++) {

        if (random_mode) {
          offset = rand() % file_size;
        }
        else{
          offset += chunk_size;
          offset %= file_size;
        }
        offset = roundup2(offset, IO_ALIGN);

        memcpy((void*) (nvm_buf + offset), buf, chunk_size);
        tm.start();

        pmem_persist(nvm_buf + offset, chunk_size);

        tm.end();
      }

      iops = (itr_cnt * 1000) / tm.duration();
      printf("\t %10.0lf ,", (iops * chunk_size) / (1024 * 1024));

      delete nvm_buf;

      tm.reset();

      // FS MODE
      path = fs_prefix + "io_file";
      int ret;

      fp = fopen(path.c_str(), "w+");
      fd = fileno(fp);
      assert(fp != NULL);
      posix_fallocate(fd, 0, file_size);

      // WRITE
      for (j = 0; j < itr_cnt; j++) {

        if (random_mode) {
          offset = rand() % file_size;
          offset = roundup2(offset, IO_ALIGN);

          tm.start();
          ret = fseek(fp, offset, SEEK_SET);
          tm.end();

          if (ret != 0) {
            perror("fseek");
            exit(EXIT_FAILURE);
          }
        }

        tm.start();
        ret = write(fd, buf, chunk_size);
        tm.end();

        if (ret <= 0) {
          perror("write");
          exit(EXIT_FAILURE);
        }

        tm.start();
        ret = fsync(fd);
        tm.end();

        if (ret != 0) {
          perror("fsync");
          exit(EXIT_FAILURE);
        }
      }

      iops = (itr_cnt * 1000) / tm.duration();
      printf("\t %10.0lf  \n", (iops * chunk_size) / (1024 * 1024));
      fclose(fp);

      tm.reset();

    }
  }

  return 0;
}
