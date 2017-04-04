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


# --[ Valgrind
find_program(MEMORYCHECK_COMMAND valgrind)
set(MEMORYCHECK_COMMAND_OPTIONS "--trace-children=yes --leak-check=full")
set(MEMORYCHECK_SUPPRESSIONS_FILE "${PROJECT_SOURCE_DIR}/third_party/valgrind/valgrind.supp")

# --[ PQXX
find_package(PQXX REQUIRED)
include_directories(SYSTEM ${PQXX_INCLUDE_DIRECTORIES})
list(APPEND Peloton_LINKER_LIBS ${PQXX_LIBRARIES})

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
