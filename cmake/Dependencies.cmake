# This list is required for static linking and exported to PelotonConfig.cmake
set(Peloton_LINKER_LIBS "")

# ---[ Boost
find_package(Boost 1.46 REQUIRED COMPONENTS system filesystem thread)
include_directories(SYSTEM ${Boost_INCLUDE_DIR})
list(APPEND Peloton_LINKER_LIBS ${Boost_LIBRARIES})

# ---[ Threads
find_package(Threads REQUIRED)
list(APPEND Peloton_LINKER_LIBS ${CMAKE_THREAD_LIBS_INIT})

# ---[ Google-gflags
include("cmake/External/gflags.cmake")
include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
list(APPEND Peloton_LINKER_LIBS ${GFLAGS_LIBRARIES})

# ---[ Google-protobuf
include(cmake/ProtoBuf.cmake)

# ---[ Libevent
find_package(Libevent REQUIRED)
include_directories(SYSTEM ${LIBEVENT_INCLUDE_DIRS})
list(APPEND Peloton_LINKER_LIBS ${LIBEVENT_LIBRARIES})
list(APPEND Peloton_LINKER_LIBS ${LIBEVENT_PTHREAD_LIBRARIES})

# ---[ Doxygen
if(BUILD_docs)
  find_package(Doxygen)
endif()

# ---[ Sanitizers
if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
   include(Sanitizer)
#  list(APPEND Peloton_LINKER_LIBS "-ltsan")
endif()

# NOTE: link jemalloc only when building binaries.
# ---[ Jemalloc
find_package(JeMalloc)
include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})

# --[ PQXX
find_package(PQXX REQUIRED)
include_directories(SYSTEM ${PQXX_INCLUDE_DIRECTORIES})
list(APPEND Peloton_LINKER_LIBS ${PQXX_LIBRARIES})

# --[ Open SSL
list(APPEND Peloton_LINKER_LIBS "-lssl")

# --[ LLVM 3.7+
find_package(LLVM 3.7 REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
llvm_map_components_to_libnames(LLVM_LIBRARIES core mcjit nativecodegen native)
include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
list(APPEND Peloton_LINKER_LIBS ${LLVM_LIBRARIES})

# --[ IWYU

# Generate clang compilation database
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PythonInterp REQUIRED)
find_program(iwyu_tool_path NAMES "${PROJECT_SOURCE_DIR}/third_party/iwyu/iwyu_tool.py")

add_custom_target(iwyu
    COMMAND "${PYTHON_EXECUTABLE}" "${iwyu_tool_path}" -p "${CMAKE_BINARY_DIR}"
    COMMENT "Running include-what-you-use tool"
    VERBATIM
)
