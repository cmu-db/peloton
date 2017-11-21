# - Try to find Liblzma
#
#
# Usage:
# LIBLZMA_INCLUDE_DIRS, where to find LibLzma headers
# LIBLZMA_LIBRARIES, LibLzma libraries
# Liblzma_FOUND, If false, do not try to use Liblzma

set(LIBLZMA_ROOT CACHE PATH "Root directory of Liblzma installation")
set(LibLzma_EXTRA_PREFIXES /usr/local /opt/local "$ENV{HOME}" ${LIBLZMA_ROOT})
foreach(prefix ${LibLzma_EXTRA_PREFIXES})
  list(APPEND LibLzma_INCLUDE_PATHS "${prefix}/include")
  list(APPEND LibLzma_LIBRARIES_PATHS "${prefix}/lib")
endforeach()

find_path(LIBLZMA_INCLUDE_DIRS event.h PATHS ${LibLzma_INCLUDE_PATHS})
# "lib" prefix is needed on Windows
find_library(LIBLZMA_LIBRARIES NAMES event Liblzma PATHS ${LibLzma_LIBRARIES_PATHS})
find_library(LIBLZMA_PTHREAD_LIBRARIES NAMES Liblzma_pthreads event_pthreads PATHS ${LibLzma_LIBRARIES_PATHS})

if (LIBLZMA_LIBRARIES AND LIBLZMA_INCLUDE_DIRS AND LIBLZMA_PTHREAD_LIBRARIES)
  set(Liblzma_FOUND TRUE)
  set(LIBLZMA_LIBRARIES ${LIBLZMA_LIBRARIES})
  set(LIBLZMA_PTHREAD_LIBRARIES ${LIBLZMA_PTHREAD_LIBRARIES})
  message(STATUS "Found Liblzma pthread (include: ${LIBLZMA_INCLUDE_DIRS}, library: ${LIBLZMA_PTHREAD_LIBRARIES})")
else ()
  set(Liblzma_FOUND FALSE)
endif ()

if (Liblzma_FOUND)
  if (NOT Liblzma_FIND_QUIETLY)
    message(STATUS "Found Liblzma (include: ${LIBLZMA_INCLUDE_DIRS}, library: ${LIBLZMA_LIBRARIES})")
  endif ()
else ()
  if (LibLzma_FIND_REQUIRED)
    message(FATAL_ERROR "Could NOT find Liblzma.")
  endif ()
  message(STATUS "Liblzma NOT found.")
endif ()

mark_as_advanced(
    LIBLZMA_LIBRARIES
    LIBLZMA_INCLUDE_DIRS
  )
