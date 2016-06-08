# Find the libunwind library
#
#  LIBUNWIND_FOUND       - True if libunwind was found.
#  LIBUNWIND_LIBRARIES   - The libraries needed to use libunwind
#  LIBUNWIND_INCLUDE_DIR - Location of unwind.h and libunwind.h
 
FIND_PATH(LIBUNWIND_INCLUDE_DIR libunwind.h)
if(NOT LIBUNWIND_INCLUDE_DIR)
  message(FATAL_ERROR "failed to find libunwind.h")
elif(NOT EXISTS "${LIBUNWIND_INCLUDE_DIR}/unwind.h")
  message(FATAL_ERROR "libunwind.h was found, but unwind.h was not found in that directory.")
  SET(LIBUNWIND_INCLUDE_DIR "")
endif()
 
FIND_LIBRARY(LIBUNWIND_GENERIC_LIBRARY "unwind")
if (NOT LIBUNWIND_GENERIC_LIBRARY)
    MESSAGE(FATAL_ERROR "failed to find unwind generic library")
endif ()
SET(LIBUNWIND_LIBRARIES ${LIBUNWIND_GENERIC_LIBRARY})

# For some reason, we have to link to two libunwind shared object files:
# one arch-specific and one not.
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
    SET(LIBUNWIND_ARCH "arm")
elseif (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
    SET(LIBUNWIND_ARCH "x86_64")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^i.86$")
    SET(LIBUNWIND_ARCH "x86")
endif()

if (LIBUNWIND_ARCH)
    FIND_LIBRARY(LIBUNWIND_SPECIFIC_LIBRARY "unwind-${LIBUNWIND_ARCH}")
    if (NOT LIBUNWIND_SPECIFIC_LIBRARY)
        MESSAGE(FATAL_ERROR "failed to find unwind-${LIBUNWIND_ARCH}")
    endif ()
    SET(LIBUNWIND_LIBRARIES ${LIBUNWIND_LIBRARIES} ${LIBUNWIND_SPECIFIC_LIBRARY})
endif(LIBUNWIND_ARCH)

MARK_AS_ADVANCED(LIBUNWIND_LIBRARIES LIBUNWIND_INCLUDE_DIR)

message(STATUS "Found libunwind (include: ${LIBUNWIND_INCLUDE_DIRS}, library: ${LIBUNWIND_LIBRARIES})")
