cmake_minimum_required(VERSION 2.8.7)

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

# ---[ Flags
if(UNIX OR APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall")
endif()

# ---[ Warnings
#peloton_warnings_disable(CMAKE_CXX_FLAGS -Wno-sign-compare -Wno-uninitialized)

# ---[ Config generation
configure_file(cmake/Templates/peloton_config.h.in "${PROJECT_BINARY_DIR}/peloton_config.h")

# ---[ Includes
set(Peloton_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(Peloton_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
include_directories(${Peloton_SRC_INCLUDE_DIR} ${Peloton_TEST_INCLUDE_DIR} ${PROJECT_BINARY_DIR})
include_directories(BEFORE src) # This is needed for gtest.

# ---[ Subdirectories
add_subdirectory(src)
#add_subdirectory(doc)

# ---[ Linter target
add_custom_target(lint COMMAND ${CMAKE_COMMAND} -P ${PROJECT_SOURCE_DIR}/cmake/lint.cmake)

# ---[ Configuration summary
peloton_print_configuration_summary()

# ---[ Export configs generation
peloton_generate_export_configs()
