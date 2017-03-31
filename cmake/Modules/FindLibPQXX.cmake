# Copyright: Andrew Maclean

# Find the LIBPQXX installation or build tree.
# The following variables are set if LIBPQXX is found.  If LIBPQXX is not
# found, LIBPQXX_FOUND is set to false.
#  LIBPQXX_FOUND         - Set to true when LIBPQXX is found.
#  LIBPQXX_INCLUDE_DIRS  - Include directories for LIBPQXX
#  LIBPQXX_LIBRARY_DIRS  - Link directories for LIBPQXX libraries
# The following cache entries must be set by the user to locate LIBPQXX:
#  LIBPQXX_DIR  - The directory containing LIBPQXXConfig.cmake.
#             This is either the root of the build tree,
#             or the lib/LIBPQXX directory.  This is the
#             only cache entry.

#  LIBPQXX_FOUND        - True when the LIBPQXX include directory is found.
#  LIBPQXX_INCLUDE_DIRS - the path to where the LIBPQXX include files are.
#  LIBPQXX_LIBRARY_DIRS - The path to where the LIBPQXX library files are.

# ----------------------------------------------------------------------------
# If you have installed LIBPQXX in a non-standard location.
# Then you have three options. 
# In the following comments, it is assumed that <Your Path>
# points to the root directory of the include directory of LIBPQXX. e.g
# If you have put LIBPQXX in C:\development\libpqxx then <Your Path> is
# "C:/development/libpqxx" and in this directory there will be two
# directories called "include" and "lib".
# 1) After CMake runs, set LIBPQXX_INCLUDE_DIR to <Your Path>/LIBPQXX<-version>
# 2) Use CMAKE_INCLUDE_PATH to set a path to <Your Path>/LIBPQXX<-version>. This will allow FIND_PATH()
#    to locate LIBPQXX_INCLUDE_DIR by utilizing the PATH_SUFFIXES option. e.g.
#    SET(CMAKE_INCLUDE_PATH ${CMAKE_INCLUDE_PATH} "<Your Path>/include")
# 3) Set an environment variable called ${LIBPQXX_ROOT} that points to the root of where you have
#    installed LIBPQXX, e.g. <Your Path>. It is assumed that there is at least a subdirectory called
#    Foundation/include/LIBPQXX in this path.
#
# Usage:
# In your CMakeLists.txt file do something like this:
# ...
# # LIBPQXX
# FIND_PACKAGE(LIBPQXX)
# ...
# INCLUDE_DIRECTORIES(${LIBPQXX_INCLUDE_DIRS})
# LINK_DIRECTORIES(${LIBPQXX_LIBRARY_DIRS})
#
# In Windows, we make the assumption that, if the LIBPQXX files are installed, the default directory
# will be C:\LIBPQXX or C:\Program Files\LIBPQXX.

set(LIBPQXX_INCLUDE_PATH_DESCRIPTION "top-level directory containing the LIBPQXX include directories. E.g /usr/local/include/libpqxx-2.6.8 or C:/libpqxx-2.6.8")
set(LIBPQXX_INCLUDE_DIR_MESSAGE "Set the LIBPQXX_INCLUDE_DIR cmake cache entry to the ${LIBPQXX_INCLUDE_PATH_DESCRIPTION}")
set(LIBPQXX_LIBRARY_PATH_DESCRIPTION "top-level directory containing the LIBPQXX libraries.")
set(LIBPQXX_LIBRARY_DIR_MESSAGE "Set the LIBPQXX_LIBRARY_DIR cmake cache entry to the ${LIBPQXX_LIBRARY_PATH_DESCRIPTION}")


set(LIBPQXX_DIR_SEARCH $ENV{LIBPQXX_ROOT})
if(LIBPQXX_DIR_SEARCH)
  file(TO_CMAKE_PATH ${LIBPQXX_DIR_SEARCH} LIBPQXX_DIR_SEARCH)
endif(LIBPQXX_DIR_SEARCH)


if(WIN32)
  set(LIBPQXX_DIR_SEARCH
    ${LIBPQXX_DIR_SEARCH}
    C:/postgresql
    D:/postgresql
    "C:Program Files/libpqxx"
    "D:Program Files/libpqxx"
  )
endif(WIN32)

# Add in some path suffixes. These will have to be updated whenever a new LIBPQXX version comes out.
set(SUFFIX_FOR_INCLUDE_PATH
 libpqxx
 libpqxx-3.0
 libpqxx-2.6.9
 libpqxx-2.6.8
)

set(SUFFIX_FOR_LIBRARY_PATH
)

#
# Look for an installation.
#
find_path(LIBPQXX_INCLUDE_DIR NAMES /include/pqxx/connection.hxx PATH_SUFFIXES ${SUFFIX_FOR_INCLUDE_PATH} PATHS

  # Look in other places.
  ${LIBPQXX_DIR_SEARCH}

  # Help the user find it if we cannot.
  DOC "The ${LIBPQXX_INCLUDE_DIR_MESSAGE}"
)

# Now try to get the include and library path.
if(LIBPQXX_INCLUDE_DIR)

  if(EXISTS "${LIBPQXX_INCLUDE_DIR}")
    set(LIBPQXX_INCLUDE_DIRS
      ${LIBPQXX_INCLUDE_DIR}/include
    )
  endif(EXISTS "${LIBPQXX_INCLUDE_DIR}")

  if(EXISTS "${LIBPQXX_INCLUDE_DIR}")
    SET(LIBPQXX_LIBRARY_DIRS
      ${LIBPQXX_INCLUDE_DIR}/lib
    )
  endif(EXISTS "${LIBPQXX_INCLUDE_DIR}")

endif(LIBPQXX_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibPQXX DEFAULT_MSG LIBPQXX_LIBRARY_DIRS LIBPQXX_INCLUDE_DIRS)