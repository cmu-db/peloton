# --[ Gmock library

# --[ Tests 

# creates 'tests' lists
peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR})

file(GLOB test_hdrs    ${PROJECT_SOURCE_DIR}/test/include/*/*.h*)
file(GLOB test_srcs    ${PROJECT_SOURCE_DIR}/test/*/*.cpp)

set(GMOCK_DIR  "${PROJECT_SOURCE_DIR}/third_party/gmock")
file(GLOB gmock_srcs   ${GMOCK_DIR}/*.cc)
include_directories(SYSTEM ${GMOCK_DIR})

add_executable(peloton-testsuite EXCLUDE_FROM_ALL ${test_srcs} ${gmock_srcs})
target_link_libraries(peloton-testsuite peloton)
set_target_properties(peloton-testsuite
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
)

# --[ Add "make check" target

add_test(peloton-testsuite peloton-testsuite)
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})
add_dependencies(check peloton-testsuite)

# ---[ Valgrind	
if (("${CMAKE_BUILD_TYPE}" STREQUAL "Valgrind") AND (NOT WIN32))
    include(MemoryCheck)
    SETUP_TARGET_FOR_MEMCHECK(valgrind ${CMAKE_BINARY_DIR}/test/peloton-testsuite)
endif()

# ---[ Coverage
if("${CMAKE_BUILD_TYPE}" STREQUAL "Coverage")
	include(CodeCoverage)	
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_COVERAGE}")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_COVERAGE}")
	SETUP_TARGET_FOR_COVERAGE(coverage peloton-testsuite coverage)
endif()	