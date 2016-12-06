# --[ Gmock library

# --[ Tests 

# creates 'tests' lists
peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR})

file(GLOB test_hdrs ${PROJECT_SOURCE_DIR}/test/include/*/*.h*)
file(GLOB test_srcs_temp ${PROJECT_SOURCE_DIR}/test/*/*test.cpp)
file(GLOB perf_srcs ${PROJECT_SOURCE_DIR}/test/performance/*.cpp)

set(test_srcs "")

foreach(test_src_temp ${test_srcs_temp} )
    string(REPLACE "//" "/" test_src ${test_src_temp})
    list(APPEND test_srcs ${test_src})
endforeach(test_src_temp ${test_srcs_temp})

# For each performance test source file
# First remove them from the test source list since they should not be run as
# part of the correctness test
foreach(perf_src ${perf_srcs} )
    list(REMOVE_ITEM test_srcs ${perf_src})
endforeach(perf_src ${perf_srcs})

set(GMOCK_DIR  "${PROJECT_SOURCE_DIR}/third_party/gmock")
file(GLOB gmock_srcs  ${GMOCK_DIR}/*.cc)
include_directories(SYSTEM ${GMOCK_DIR})

set(HARNESS ${PROJECT_SOURCE_DIR}/test/common/harness.cpp)
set(EXECUTOR_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/executor/executor_tests_util.cpp)
set(LOGGING_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/logging/logging_tests_util.cpp)
set(JOIN_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/executor/join_tests_util.cpp)
set(TXN_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/concurrency/transaction_tests_util.cpp)
set(STATS_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/statistics/stats_tests_util.cpp)
set(SQL_TESTS_UTIL ${PROJECT_SOURCE_DIR}/test/sql/sql_tests_util.cpp)

add_library(peloton-test-common EXCLUDE_FROM_ALL ${gmock_srcs} 
            ${HARNESS} ${EXECUTOR_TESTS_UTIL} ${LOGGING_TESTS_UTIL} ${JOIN_TESTS_UTIL}
            ${TXN_TESTS_UTIL} ${STATS_TESTS_UTIL} ${SQL_TESTS_UTIL})

# --[ Add "make check" target

set(CTEST_FLAGS "")
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} ${CTEST_FLAGS} --verbose)

# Set memcheck parameters
find_program(MEMORYCHECK_COMMAND valgrind REQUIRED)
set(MEMORYCHECK_COMMAND_OPTIONS --trace-children=yes --leak-check=full --track-origins=yes
    --soname-synonyms=somalloc=*jemalloc* --error-exitcode=1)
set(MEMORYCHECK_SUPPRESSIONS_FILE ${PROJECT_SOURCE_DIR}/third_party/valgrind/valgrind.supp)

foreach(test_src ${test_srcs} )
    #message("test_src = " ${test_src})
    
    # get test file name
	get_filename_component(test_bare_name ${test_src} NAME)	
	string(REPLACE ".cpp" "" test_bare_name_without_extension ${test_bare_name})
    string(REPLACE "\"" "" test_name ${test_bare_name_without_extension})
     
    # create executable
    add_executable(${test_name} EXCLUDE_FROM_ALL ${test_src})
    add_dependencies(check ${test_name})
    
    #message("Correctness test: "  ${test_name})
    
    # link libraries
    target_link_libraries(${test_name} peloton peloton-test-common)    

	# set target properties    
	set_target_properties(${test_name}
	    PROPERTIES
	    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
	    COMMAND ${test_name}
	)   
	
	# add test
    add_test(${test_name} ${MEMORYCHECK_COMMAND} ${MEMORYCHECK_COMMAND_OPTIONS}
        --suppressions=${MEMORYCHECK_SUPPRESSIONS_FILE} ${CMAKE_BINARY_DIR}/test/${test_name}
        --gtest_output=xml:${CMAKE_BINARY_DIR}/test/unit_${test_name}.xml)
	
endforeach(test_src ${test_srcs})

# Compile and run performance tests separately
foreach(perf_src ${perf_srcs} )
    list(REMOVE_ITEM test_srcs ${perf_src})
    
    #message("test_srcs = " ${test_srcs})
    #message("perf_src = " ${perf_src})
    
    get_filename_component(perf_bare_name ${perf_src} NAME)	
	string(REPLACE ".cpp" "" perf_bare_name_without_extension ${perf_bare_name})
    string(REPLACE "\"" "" perf_name ${perf_bare_name_without_extension})
     
    # create executable
    add_executable(${perf_name} EXCLUDE_FROM_ALL ${perf_src})
    add_dependencies(check ${perf_name})
    
    #message("Performance test: "  ${perf_name})
    
    # link libraries
    target_link_libraries(${perf_name} peloton peloton-test-common)    

	# set target properties    
	set_target_properties(${perf_name}
	    PROPERTIES
	    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
	    COMMAND ${perf_name}
	)   
	
	# add test
	add_test(${perf_name} ${CMAKE_BINARY_DIR}/test/${perf_name}
        --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${perf_name}.xml)
	
endforeach(perf_src ${perf_srcs})
