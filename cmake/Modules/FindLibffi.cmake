# - Try to find Libffi
#
#  A Portable Foreign Function Interface Library (https://sourceware.org/libffi)
#
# Usage:
# LIBFFI_INCLUDE_DIRS, location of header files
# LIBFFI_LIBRARIES, location of library
# LIBFFI_FOUND, indicates if libffi was found

# Look for the header file.
execute_process(COMMAND brew --prefix libffi OUTPUT_VARIABLE LIBFFI_BREW_PREFIX)

find_library(LIBFFI_LIBRARY NAMES ffi libffi
        PATHS /usr /usr/local /opt/local
        PATH_SUFFIXES lib lib64 x86_64-linux-gnu lib/x86_64-linux-gnu
        )

find_path(LIBFFI_INCLUDE_DIR ffi.h
        PATHS /usr /usr/local /opt/local /usr/include/ffi
        PATH_SUFFIXES include include/ffi include/x86_64-linux-gnu x86_64-linux-gnu
        HINT LIBFFI_BREW_PREFIX
        )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LIBFFI DEFAULT_MSG LIBFFI_LIBRARY LIBFFI_INCLUDE_DIR)


# Copy the results to the output variables.
IF(LIBFFI_FOUND)
    SET(LIBFFI_LIBRARIES ${LIBFFI_LIBRARY})
    SET(LIBFFI_INCLUDE_DIRS ${LIBFFI_INCLUDE_DIR})
ELSE(LIBFFI_FOUND)
    SET(LIBFFI_LIBRARIES)
    SET(LIBFFI_INCLUDE_DIRS)
ENDIF(LIBFFI_FOUND)

MARK_AS_ADVANCED(LIBFFI_INCLUDE_DIRS LIBFFI_LIBRARIES)

message(STATUS "Found Libffi (include: ${LIBFFI_INCLUDE_DIRS}, library: ${LIBFFI_LIBRARIES})")