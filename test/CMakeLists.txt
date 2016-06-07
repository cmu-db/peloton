# --[ Gmock library

# --[ Tests 

# creates 'tests' lists
peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR})

file(GLOB test_hdrs ${PROJECT_SOURCE_DIR}/test/include/*/*.h*)
file(GLOB test_srcs ${PROJECT_SOURCE_DIR}/test/*/*.cpp)

set(GMOCK_DIR  "${PROJECT_SOURCE_DIR}/third_party/gmock")
file(GLOB gmock_srcs  ${GMOCK_DIR}/*.cc)
include_directories(SYSTEM ${GMOCK_DIR})

set(HARNESS ${PROJECT_SOURCE_DIR}/test/common/harness.cpp)
set(EXECUTOR_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/executor/executor_tests_util.cpp)
set(LOGGING_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/logging/logging_tests_util.cpp)
set(JOIN_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/executor/join_tests_util.cpp)
set(TXN_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/concurrency/transaction_tests_util.cpp)

add_library(peloton-test-common EXCLUDE_FROM_ALL ${gmock_srcs} 
            ${HARNESS} ${EXECUTOR_TESTS_UTIL} ${LOGGING_TESTS_UTIL} ${JOIN_TESTS_UTIL} ${TXN_TESTS_UTIL})

# --[ Add "make check" target

add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

foreach(test_src ${test_srcs} )
    # get test file name
	get_filename_component(test_bare_name ${test_src} NAME)	
	string(REPLACE ".cpp" "" test_name ${test_bare_name} )
    
    # create executable
    add_executable(${test_name} EXCLUDE_FROM_ALL ${test_src})
    add_dependencies(check ${test_name})
    
    # link libraries
    target_link_libraries(${test_name} peloton peloton-test-common)    
    
	set_target_properties(${test_name}
	    PROPERTIES
	    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
	)    
	
	# add test
	add_test(${test_name} ${test_name})
endforeach(test_src ${test_srcs})

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