# --[ Proto library
file(GLOB proto_files proto/*.proto)
peloton_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python ${proto_files})

# include python files either to force generation
add_library(peloton-proto STATIC ${proto_hdrs} ${proto_srcs} ${proto_python})
set(Peloton_LINKER_LIBS peloton-proto ${Peloton_LINKER_LIBS}) # note, crucial to prepend!
peloton_default_properties(peloton-proto)

# --[ Postgres files

file(GLOB_RECURSE postgres_srcs ${PROJECT_SOURCE_DIR}/src/parser/*.cpp)
set(POSTGRES_FLAGS "-w -Wno-error -fpermissive")

foreach(postgres_src ${postgres_srcs} )
	set_source_files_properties(${postgres_src} PROPERTIES COMPILE_FLAGS ${POSTGRES_FLAGS})
endforeach(postgres_src ${postgres_srcs})    

# --[ Peloton library

# creates 'srcs' lists
peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR})

add_library(peloton SHARED ${srcs})

target_link_libraries(peloton ${Peloton_LINKER_LIBS} peloton-proto)

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
add_executable(hyadapt EXCLUDE_FROM_ALL ${hyadapt_srcs})
target_link_libraries(hyadapt peloton)

# --[ ycsb
file(GLOB_RECURSE ycsb_srcs ${PROJECT_SOURCE_DIR}/src/main/ycsb/*.cpp)
add_executable(ycsb EXCLUDE_FROM_ALL ${ycsb_srcs})
target_link_libraries(ycsb peloton)

# --[ tpcc
file(GLOB_RECURSE tpcc_srcs ${PROJECT_SOURCE_DIR}/src/main/tpcc/*.cpp)
add_executable(tpcc EXCLUDE_FROM_ALL ${tpcc_srcs})
target_link_libraries(tpcc peloton)

# --[ sdbench
file(GLOB_RECURSE sdbench_srcs ${PROJECT_SOURCE_DIR}/src/main/sdbench/*.cpp)
add_executable(sdbench EXCLUDE_FROM_ALL ${sdbench_srcs})
target_link_libraries(sdbench peloton)


# --[ logger
file(GLOB_RECURSE logger_srcs ${PROJECT_SOURCE_DIR}/src/main/logger/*.cpp)
list(APPEND logger_srcs ${ycsb_srcs})
list(REMOVE_ITEM  logger_srcs ${PROJECT_SOURCE_DIR}/src/main/ycsb/ycsb.cpp)
list(APPEND logger_srcs ${tpcc_srcs})
list(REMOVE_ITEM  logger_srcs ${PROJECT_SOURCE_DIR}/src/main/tpcc/tpcc.cpp)
add_executable(logger EXCLUDE_FROM_ALL ${logger_srcs})
target_link_libraries(logger peloton)

# --[ benchmark

add_custom_target(benchmark)

add_dependencies(benchmark hyadapt ycsb tpcc sdbench logger)


