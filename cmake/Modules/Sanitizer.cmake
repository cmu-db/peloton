# Support for building with ASAN and TSAN -
# https://code.google.com/p/thread-sanitizer/

set(PELOTON_USE_TSAN "OFF")

# Clang does not support using ASAN and TSAN simultaneously.
if ("${PELOTON_USE_ASAN}" AND "${PELOTON_USE_TSAN}")
  message(SEND_ERROR "Can only enable one of ASAN or TSAN at a time")
endif()

# Flag to enable clang address sanitizer
# This will only build if clang or a recent enough gcc is the chosen compiler
if (${PELOTON_USE_ASAN})
  if(NOT (("${COMPILER_FAMILY}" STREQUAL "clang") OR
          ("${COMPILER_FAMILY}" STREQUAL "gcc" AND "${COMPILER_VERSION}" VERSION_GREATER "4.8")))
    message(SEND_ERROR "Cannot use ASAN without clang or gcc >= 4.8")
  endif()

  # If UBSAN is also enabled, and we're on clang < 3.5, ensure static linking is
  # enabled. Otherwise, we run into https://llvm.org/bugs/show_bug.cgi?id=18211
  if("${PELOTON_USE_UBSAN}" AND
      "${COMPILER_FAMILY}" STREQUAL "clang" AND
      "${COMPILER_VERSION}" VERSION_LESS "3.5")
    if("${PELOTON_LINK}" STREQUAL "a")
      message("Using static linking for ASAN+UBSAN build")
      set(PELOTON_LINK "s")
    elseif("${PELOTON_LINK}" STREQUAL "d")
      message(SEND_ERROR "Cannot use dynamic linking when ASAN and UBSAN are both enabled")
    endif()
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -DADDRESS_SANITIZER")
endif()


# Flag to enable clang undefined behavior sanitizer
# We explicitly don't enable all of the sanitizer flags:
# - disable 'vptr' because it currently crashes somewhere in boost::intrusive::list code
# - disable 'alignment' because unaligned access is really OK on Nehalem and we do it
#   all over the place.
if (${PELOTON_USE_UBSAN})
  if(NOT (("${COMPILER_FAMILY}" STREQUAL "clang") OR
          ("${COMPILER_FAMILY}" STREQUAL "gcc" AND "${COMPILER_VERSION}" VERSION_GREATER "4.9")))
    message(SEND_ERROR "Cannot use UBSAN without clang or gcc >= 4.9")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr -fno-sanitize-recover")
endif ()

# Flag to enable thread sanitizer (clang or gcc 4.8)
if (${PELOTON_USE_TSAN})
  if(NOT (("${COMPILER_FAMILY}" STREQUAL "clang") OR
          ("${COMPILER_FAMILY}" STREQUAL "gcc" AND "${COMPILER_VERSION}" VERSION_GREATER "4.8")))
    message(SEND_ERROR "Cannot use TSAN without clang or gcc >= 4.8")
  endif()

  SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fsanitize=thread")

  # Enables dynamic_annotations.h to actually generate code
  SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DDYNAMIC_ANNOTATIONS_ENABLED")

  # changes atomicops to use the tsan implementations
  SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DTHREAD_SANITIZER")
  
  # Disables using the precompiled template specializations for std::string, shared_ptr, etc
  # so that the annotations in the header actually take effect.
  SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -D_GLIBCXX_EXTERN_TEMPLATE=0")

  set(TSAN_LIBRARIES "-ltsan")

  # Some of the above also need to be passed to the linker.
  set(CMAKE_EXE_LINKER_FLAGS "-pie -fsanitize=thread ${TSAN_LIBRARIES} ${CMAKE_EXE_LINKER_FLAGS}")

  message(STATUS "CMAKE_EXE_LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}")
  message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

  # Strictly speaking, TSAN doesn't require dynamic linking. But it does
  # require all code to be position independent, and the easiest way to
  # guarantee that is via dynamic linking (not all 3rd party archives are
  # compiled with -fPIC e.g. boost).
  if("${PELOTON_LINK}" STREQUAL "a")
    message("Using dynamic linking for TSAN")
    set(PELOTON_LINK "d")
  elseif("${PELOTON_LINK}" STREQUAL "s")
    message(SEND_ERROR "Cannot use TSAN with static linking")
  endif()
endif()


if ("${PELOTON_USE_UBSAN}" OR "${PELOTON_USE_ASAN}" OR "${PELOTON_USE_TSAN}")
  # GCC 4.8 and 4.9 (latest as of this writing) don't allow you to specify a
  # sanitizer blacklist.
  if("${COMPILER_FAMILY}" STREQUAL "clang")
    # Require clang 3.4 or newer; clang 3.3 has issues with TSAN and pthread
    # symbol interception.
    if("${COMPILER_VERSION}" VERSION_LESS "3.4")
      message(SEND_ERROR "Must use clang 3.4 or newer to run a sanitizer build."
        " Try using clang from $NATIVE_TOOLCHAIN/")
    endif()
    
    SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fsanitize-blacklist=${BUILD_SUPPORT_DIR}/sanitize-blacklist.txt")
  else()
    #message(WARNING "GCC does not support specifying a sanitizer blacklist. Known sanitizer check failures will not be suppressed.")
  endif()
endif()
