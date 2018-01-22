##################################################################################
# SRC CMAKELISTS
##################################################################################

# --[ Cap'nProto library

file(GLOB capnp_files include/capnp/*.capnp)
if (capnp_files)
  # the output folder for the compiled capnp headers and srouces
  set(CAPNPC_OUTPUT_DIR ${PROJECT_BINARY_DIR}/include/peloton/capnp)
  capnp_generate_cpp(capnp_srcs capnp_hdrs ${capnp_files})

  add_library(peloton-capnp STATIC ${capnp_hdrs} ${capnp_srcs})
  set(Peloton_LINKER_LIBS peloton-capnp ${Peloton_LINKER_LIBS}) # note, crucial to prepend!
  peloton_default_properties(peloton-capnp)
endif()

##################################################################################

# --[ Proto library

file(GLOB proto_files include/proto/*.proto)
peloton_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python ${proto_files})

# include python files either to force generation
add_library(peloton-proto STATIC ${proto_hdrs} ${proto_srcs} ${proto_python})
target_compile_options(peloton-proto PRIVATE "-Wno-unused-parameter")
set(Peloton_LINKER_LIBS peloton-proto ${Peloton_LINKER_LIBS}) # note, crucial to prepend!
peloton_default_properties(peloton-proto)

##################################################################################

# --[ Libpg_query library

add_custom_target(libpg_query ALL
                  COMMAND ${CMAKE_MAKE_PROGRAM}
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/libpg_query/
                  COMMENT "Original libpg makefile target")


##################################################################################

# --[ Peloton library

# creates 'srcs' lists
peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR})

add_library(peloton SHARED ${srcs})
add_dependencies(peloton libpg_query)
target_link_libraries(peloton ${Peloton_LINKER_LIBS}
    ${PROJECT_SOURCE_DIR}/third_party/libpg_query/libpg_query.a)

peloton_default_properties(peloton)
set_target_properties(peloton PROPERTIES
    VERSION   ${PELOTON_TARGET_VERSION}
    SOVERSION ${PELOTON_TARGET_SOVERSION}
)

##################################################################################

# --[ Peloton

set(peloton_main ${PROJECT_SOURCE_DIR}/src/main/peloton/peloton.cpp)
add_executable(peloton-bin ${peloton_main})
target_link_libraries(peloton-bin peloton)
set_target_properties(peloton-bin PROPERTIES OUTPUT_NAME peloton)

##################################################################################

# --[ Benchmarks

# --[ tpcc
file(GLOB_RECURSE tpcc_srcs ${PROJECT_SOURCE_DIR}/src/main/tpcc/*.cpp)
add_executable(tpcc EXCLUDE_FROM_ALL ${tpcc_srcs})
target_link_libraries(tpcc peloton)

# --[ ycsb
file(GLOB_RECURSE ycsb_srcs ${PROJECT_SOURCE_DIR}/src/main/ycsb/*.cpp)
add_executable(ycsb EXCLUDE_FROM_ALL ${ycsb_srcs})
target_link_libraries(ycsb peloton)

# --[ sdbench
file(GLOB_RECURSE sdbench_srcs ${PROJECT_SOURCE_DIR}/src/main/sdbench/*.cpp)
add_executable(sdbench EXCLUDE_FROM_ALL ${sdbench_srcs})
target_link_libraries(sdbench peloton)

# --[ tpch
file(GLOB_RECURSE tpch_srcs ${PROJECT_SOURCE_DIR}/src/main/tpch/*.cpp)
add_executable(tpch EXCLUDE_FROM_ALL ${tpch_srcs})
target_link_libraries(tpch peloton)

# --[ logger
#file(GLOB_RECURSE logger_srcs ${PROJECT_SOURCE_DIR}/src/main/logger/*.cpp)
#list(APPEND logger_srcs ${ycsb_srcs})
#list(REMOVE_ITEM  logger_srcs ${PROJECT_SOURCE_DIR}/src/main/ycsb/ycsb.cpp)
#list(APPEND logger_srcs ${tpcc_srcs})
#list(REMOVE_ITEM  logger_srcs ${PROJECT_SOURCE_DIR}/src/main/tpcc/tpcc.cpp)
#add_executable(logger EXCLUDE_FROM_ALL ${logger_srcs})
#target_link_libraries(logger peloton)

# --[ link to jemalloc
set(EXE_LINK_LIBRARIES ${JEMALLOC_LIBRARIES})
set(EXE_LINK_FLAGS "-Wl,--no-as-needed")
set(EXE_LIST peloton-bin ycsb tpcc sdbench tpch)
foreach(exe_name ${EXE_LIST})
    target_link_libraries(${exe_name} ${EXE_LINK_LIBRARIES})
    if (LINUX)
        set_target_properties(${exe_name} PROPERTIES LINK_FLAGS ${EXE_LINK_FLAGS})
    endif ()
endforeach()

# --[ benchmark

add_custom_target(benchmark)
add_dependencies(benchmark tpcc ycsb sdbench)

