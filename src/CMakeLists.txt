# generate protobuf sources
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
target_link_libraries(peloton proto ${Peloton_LINKER_LIBS})
peloton_default_properties(peloton)
set_target_properties(peloton PROPERTIES
    VERSION   ${PELOTON_TARGET_VERSION}
    SOVERSION ${PELOTON_TARGET_SOVERSION}
    )

file(GLOB_RECURSE foo_src ${PROJECT_SOURCE_DIR}/benchmark/peloton/src/*.cpp)    
MESSAGE( STATUS, ${foo_src})

ADD_EXECUTABLE(foo ${foo_src})
TARGET_LINK_LIBRARIES(foo ${PROJECT_BINARY_DIR}/lib/libpeloton.a)