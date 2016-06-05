# --[ BISON
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

BISON_TARGET(SQLParser ${PROJECT_SOURCE_DIR}/src/parser/sql_parser.y ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.cpp)
FLEX_TARGET(SQLScanner ${PROJECT_SOURCE_DIR}/src/parser/sql_scanner.l ${CMAKE_CURRENT_BINARY_DIR}/sql_scanner.cpp)
ADD_FLEX_BISON_DEPENDENCY(SQLScanner SQLParser)

add_library(peloton-parser STATIC ${BISON_SQLParser_OUTPUTS} ${FLEX_SQLScanner_OUTPUTS})
set(Peloton_LINKER_LIBS peloton-parser ${Peloton_LINKER_LIBS}) # note, crucial to prepend!
peloton_default_properties(peloton-parser)

# --[ Proto library
file(GLOB proto_files proto/*.proto)
peloton_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python ${proto_files})

# include python files either to force generation
add_library(peloton-proto STATIC ${proto_hdrs} ${proto_srcs} ${proto_python})
set(Peloton_LINKER_LIBS peloton-proto ${Peloton_LINKER_LIBS}) # note, crucial to prepend!
peloton_default_properties(peloton-proto)

# --[ Peloton library

# creates 'srcs' lists
peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR})

add_library(peloton SHARED ${srcs})

target_link_libraries(peloton ${Peloton_LINKER_LIBS} peloton-parser peloton-proto)

peloton_default_properties(peloton)
set_target_properties(peloton PROPERTIES
    VERSION   ${PELOTON_TARGET_VERSION}
    SOVERSION ${PELOTON_TARGET_SOVERSION}
    )

# --[ Peloton
set(peloton_main ${PROJECT_SOURCE_DIR}/src/main/peloton/peloton.cpp)
add_executable(peloton-bin ${peloton_main})
target_link_libraries(peloton-bin peloton)
set_target_properties(peloton-bin PROPERTIES OUTPUT_NAME peloton)

# --[ Benchmarks

# --[ hyadapt
file(GLOB_RECURSE hyadapt_srcs ${PROJECT_SOURCE_DIR}/src/main/hyadapt/*.cpp)
add_executable(hyadapt ${hyadapt_srcs})
target_link_libraries(hyadapt peloton)

# --[ ycsb
file(GLOB_RECURSE ycsb_srcs ${PROJECT_SOURCE_DIR}/src/main/ycsb/*.cpp)
add_executable(ycsb ${ycsb_srcs})
target_link_libraries(ycsb peloton)

# --[ tpcc
file(GLOB_RECURSE tpcc_srcs ${PROJECT_SOURCE_DIR}/src/main/tpcc/*.cpp)
add_executable(tpcc ${tpcc_srcs})
target_link_libraries(tpcc peloton)

# --[ sdbench
file(GLOB_RECURSE sdbench_srcs ${PROJECT_SOURCE_DIR}/src/main/sdbench/*.cpp)
add_executable(sdbench ${sdbench_srcs})
target_link_libraries(sdbench peloton)

# --[ logger
file(GLOB_RECURSE logger_srcs ${PROJECT_SOURCE_DIR}/src/main/logger/*.cpp)
list(APPEND logger_srcs ${ycsb_srcs})
list(REMOVE_ITEM  logger_srcs ${PROJECT_SOURCE_DIR}/src/main/ycsb/ycsb.cpp)
list(APPEND logger_srcs ${tpcc_srcs})
list(REMOVE_ITEM  logger_srcs ${PROJECT_SOURCE_DIR}/src/main/tpcc/tpcc.cpp)
add_executable(logger ${logger_srcs})
target_link_libraries(logger peloton)
