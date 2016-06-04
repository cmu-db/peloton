# This list is required for static linking and exported to PelotonConfig.cmake
set(Peloton_LINKER_LIBS "")

# ---[ Boost
find_package(Boost 1.46 REQUIRED COMPONENTS system filesystem)
include_directories(SYSTEM ${Boost_INCLUDE_DIR})
list(APPEND Peloton_LINKER_LIBS ${Boost_LIBRARIES})

# ---[ Threads
find_package(Threads REQUIRED)
list(APPEND Peloton_LINKER_LIBS ${CMAKE_THREAD_LIBS_INIT})

# ---[ Google-glog
include("cmake/External/glog.cmake")
include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
list(APPEND Peloton_LINKER_LIBS ${GLOG_LIBRARIES})

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

# ---[ Jsoncpp
find_package(Jsoncpp)
include_directories(SYSTEM ${JSONCPP_INCLUDE_DIRS})
list(APPEND Peloton_LINKER_LIBS ${JSONCPP_LIBRARIES})

# ---[ Sanitizers
if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  find_package(Sanitizer)
  list(APPEND Peloton_LINKER_LIBS "-ltsan")
endif()
