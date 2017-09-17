# - Try to find Libevent
#
# an event notification library (http://libevent.org/)
#
# Usage: 
# LIBEVENT_INCLUDE_DIRS, where to find LibEvent headers
# LIBEVENT_LIBRARIES, LibEvent libraries
# Libevent_FOUND, If false, do not try to use libevent

set(LIBEVENT_ROOT CACHE PATH "Root directory of libevent installation")
set(LibEvent_EXTRA_PREFIXES /usr/local /opt/local "$ENV{HOME}" ${LIBEVENT_ROOT})
foreach(prefix ${LibEvent_EXTRA_PREFIXES})
  list(APPEND LibEvent_INCLUDE_PATHS "${prefix}/include")
  list(APPEND LibEvent_LIBRARIES_PATHS "${prefix}/lib")
endforeach()

find_path(LIBEVENT_INCLUDE_DIRS event.h PATHS ${LibEvent_INCLUDE_PATHS})
# "lib" prefix is needed on Windows
find_library(LIBEVENT_LIBRARIES NAMES event libevent PATHS ${LibEvent_LIBRARIES_PATHS})
find_library(LIBEVENT_PTHREAD_LIBRARIES NAMES libevent_pthreads event_pthreads PATHS ${LibEvent_LIBRARIES_PATHS})

if (LIBEVENT_LIBRARIES AND LIBEVENT_INCLUDE_DIRS AND LIBEVENT_PTHREAD_LIBRARIES)
  set(Libevent_FOUND TRUE)
  set(LIBEVENT_LIBRARIES ${LIBEVENT_LIBRARIES})
  set(LIBEVENT_PTHREAD_LIBRARIES ${LIBEVENT_PTHREAD_LIBRARIES})
  message(STATUS "Found libevent pthread (include: ${LIBEVENT_INCLUDE_DIRS}, library: ${LIBEVENT_PTHREAD_LIBRARIES})")
else ()
  set(Libevent_FOUND FALSE)
endif ()

if (Libevent_FOUND)
  if (NOT Libevent_FIND_QUIETLY)
    message(STATUS "Found libevent (include: ${LIBEVENT_INCLUDE_DIRS}, library: ${LIBEVENT_LIBRARIES})")
  endif ()
else ()
  if (LibEvent_FIND_REQUIRED)
    message(FATAL_ERROR "Could NOT find libevent.")
  endif ()
  message(STATUS "libevent NOT found.")
endif ()

mark_as_advanced(
    LIBEVENT_LIBRARIES
    LIBEVENT_INCLUDE_DIRS
  )
