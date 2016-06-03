cmake_minimum_required(VERSION 2.8.7)
if(POLICY CMP0046)
  cmake_policy(SET CMP0046 NEW)
endif()
if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()

# ---[ Peloton project
project(Peloton CXX C)

# ---[ Peloton version
set(PELOTON_TARGET_VERSION "0.0.3" CACHE STRING "Peloton logical version")
set(PELOTON_TARGET_SOVERSION "0.0.3" CACHE STRING "Peloton soname version")
add_definitions(-DPELOTON_VERSION=${PELOTON_TARGET_VERSION})

# ---[ Using cmake scripts and modules
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

include(ExternalProject)

include(cmake/Utils.cmake)
include(cmake/Targets.cmake)
include(cmake/Misc.cmake)
include(cmake/Summary.cmake)
include(cmake/ConfigGen.cmake)

# ---[ Options
peloton_option(BUILD_docs   "Build documentation" ON IF UNIX OR APPLE)

# ---[ Dependencies
include(cmake/Dependencies.cmake)

# ---[ C++11
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()

# ---[ Flags
if(UNIX OR APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -Wextra -Werror")
endif()

# ---[ Warnings
peloton_warnings_disable(CMAKE_CXX_FLAGS -Wno-strict-aliasing)

# ---[ Config generation
configure_file(cmake/Templates/peloton_config.h.in "${PROJECT_BINARY_DIR}/peloton_config.h")

# ---[ Includes
set(Peloton_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(Peloton_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(Peloton_THIRD_PARTY_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party)
include_directories(${Peloton_SRC_INCLUDE_DIR} ${Peloton_TEST_INCLUDE_DIR} ${Peloton_THIRD_PARTY_INCLUDE_DIR} ${PROJECT_BINARY_DIR})
include_directories(BEFORE src) # This is needed for gtest.

# ---[ Subdirectories
add_subdirectory(src)
#add_subdirectory(doc)

#file(GLOB_RECURSE hyadapt_files ${root}/benchmark/hyadapt/src/*.cpp)    
#ADD_EXECUTABLE(hyadapt ${hyadapt_files})
#TARGET_LINK_LIBRARIES(hyadapt peloton)

# ---[ Linter target
add_custom_target(lint COMMAND ${CMAKE_COMMAND} -P ${PROJECT_SOURCE_DIR}/cmake/lint.cmake)

# ---[ Configuration summary
peloton_print_configuration_summary()

# ---[ Export configs generation
peloton_generate_export_configs()
