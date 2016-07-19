#
# - Enable Valgrind Check
#
#   Build a Valgrind build:
#     cmake -DCMAKE_BUILD_TYPE=Valgrind ..
#     make
#     make _targetname
#
#

# Check prereqs
FIND_PROGRAM( VALGRIND_PATH valgrind )

IF(NOT VALGRIND_PATH)
    MESSAGE(FATAL_ERROR "valgrind not found! Aborting...")
ENDIF() # NOT VALGRIND_PATH

SET(VALGRIND_OPTIONS "")

SET(CMAKE_CXX_FLAGS_MEMCHECK
    "-g -O0 -fprofile-arcs "
    CACHE STRING "Flags used by the C++ compiler during valgrind builds."
    FORCE )
SET(CMAKE_C_FLAGS_MEMCHECK
    "-g -O0 -fprofile-arcs"
    CACHE STRING "Flags used by the C compiler during valgrind builds."
    FORCE )
SET(CMAKE_EXE_LINKER_FLAGS_MEMCHECK
    ""
    CACHE STRING "Flags used for linking binaries during valgrind builds."
    FORCE )
SET(CMAKE_SHARED_LINKER_FLAGS_MEMCHECK
    ""
    CACHE STRING "Flags used by the shared libraries linker during valgrind builds."
    FORCE )
MARK_AS_ADVANCED(
    CMAKE_CXX_FLAGS_MEMCHECK
    CMAKE_C_FLAGS_MEMCHECK
    CMAKE_EXE_LINKER_FLAGS_MEMCHECK
    CMAKE_SHARED_LINKER_FLAGS_MEMCHECK )

IF ( NOT (CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "Valgrind"))
  MESSAGE( WARNING "Valgrind results with an optimized (non-Debug) build may be misleading" )
ENDIF() # NOT CMAKE_BUILD_TYPE STREQUAL "Debug"


# Param _targetname     The name of new the custom make target
# Param list of target tests
FUNCTION(SETUP_TARGET_FOR_MEMCHECK _targetname _test)

    IF(NOT VALGRIND_PATH)
        MESSAGE(FATAL_ERROR "valgrind not found! Aborting...")
    ENDIF() # NOT VALGRIND_PATH

    # Setup target
    ADD_CUSTOM_TARGET(${_targetname}

    COMMENT "Test: ${I}"

        # Run tests
    COMMAND ${VALGRIND_PATH} --trace-children=yes --quiet --tool=memcheck
                             --leak-check=yes --show-reachable=yes
                             --num-callers=100 --verbose --demangle=yes
                             ${_test}

    COMMENT "Valgrind run complete for ${_test}"
    )

ENDFUNCTION() # SETUP_TARGET_FOR_MEMCHECK
