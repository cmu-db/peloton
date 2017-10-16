/* Sources directory */
#define SOURCE_FOLDER "/Users/Tianyu/Desktop/peloton"

/* Binaries directory */
#define BINARY_FOLDER "/Users/Tianyu/Desktop/peloton/cmake"

/* Temporary (TODO: remove) */
#if 1
  #define CMAKE_SOURCE_DIR SOURCE_FOLDER "/src/"
  #define EXAMPLES_SOURCE_DIR BINARY_FOLDER "/examples/"
  #define CMAKE_EXT ".gen.cmake"
#else
  #define CMAKE_SOURCE_DIR "src/"
  #define EXAMPLES_SOURCE_DIR "examples/"
  #define CMAKE_EXT ""
#endif
