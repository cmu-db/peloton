# --[ Proto library
file(GLOB proto_files proto/*.proto)
peloton_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python ${proto_files})

# include python files either to force generation
add_library(proto STATIC ${proto_hdrs} ${proto_srcs} ${proto_python})
set(Peloton_LINKER_LIBS proto ${Peloton_LINKER_LIBS}) # note, crucial to prepend!
peloton_default_properties(proto)

# --[ Peloton library

# creates 'srcs' lists
peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR})

add_library(peloton ${srcs})
target_link_libraries(peloton ${Peloton_LINKER_LIBS} proto)

peloton_default_properties(peloton)
set_target_properties(peloton PROPERTIES
    VERSION   ${PELOTON_TARGET_VERSION}
    SOVERSION ${PELOTON_TARGET_SOVERSION}
    )

# --[ Peloton
set(peloton_main ${PROJECT_SOURCE_DIR}/src/main/peloton.cpp)
add_executable(peloton-bin ${peloton_main})
target_link_libraries(peloton-bin peloton)
set_target_properties(peloton-bin PROPERTIES OUTPUT_NAME peloton)

# --[ Benchmarks

# --[ hyadapt
set(hyadapt_main ${PROJECT_SOURCE_DIR}/src/benchmark/hyadapt/hyadapt.cpp)
add_executable(hyadapt ${hyadapt_main})
target_link_libraries(hyadapt peloton)

# --[ ycsb
set(ycsb_main ${PROJECT_SOURCE_DIR}/src/benchmark/ycsb/ycsb.cpp)
add_executable(ycsb ${ycsb_main})
target_link_libraries(ycsb peloton)

# --[ tpcc
set(tpcc_main ${PROJECT_SOURCE_DIR}/src/benchmark/tpcc/tpcc.cpp)
add_executable(tpcc ${tpcc_main})
target_link_libraries(tpcc peloton)

# --[ sdbench
set(sdbench_main ${PROJECT_SOURCE_DIR}/src/benchmark/sdbench/sdbench.cpp)
add_executable(sdbench ${sdbench_main})
target_link_libraries(sdbench peloton)

# --[ logger
set(logger_main ${PROJECT_SOURCE_DIR}/src/benchmark/logger/logger.cpp)
add_executable(logger ${logger_main})
target_link_libraries(logger peloton)