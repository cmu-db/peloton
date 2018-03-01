# Config file for the Peloton package.
#
# After successful configuration the following variables
# will be defined:
#
#   Peloton_INCLUDE_DIRS - Peloton include directories
#   Peloton_LIBRARIES    - libraries to link against
#   Peloton_DEFINITIONS  - a list of definitions to pass to compiler
#

# Compute paths
get_filename_component(Peloton_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
set(Peloton_INCLUDE_DIRS "/usr/local/include;/usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/include")

get_filename_component(__peloton_include "${Peloton_CMAKE_DIR}/../../include" ABSOLUTE)
list(APPEND Peloton_INCLUDE_DIRS ${__peloton_include})
unset(__peloton_include)


# Our library dependencies
if(NOT TARGET peloton AND NOT peloton_BINARY_DIR)
  include("${Peloton_CMAKE_DIR}/PelotonTargets.cmake")
endif()

# List of IMPORTED libs created by PelotonTargets.cmake
set(Peloton_LIBRARIES peloton)

# Definitions
set(Peloton_DEFINITIONS "")
