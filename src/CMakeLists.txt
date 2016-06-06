# --[ ANTLR

find_package(Java REQUIRED)

set(SQL_PARSER_LOCATION "${PROJECT_SOURCE_DIR}/src/parser/parser/antlr")
set(SQL_PARSER_GRAMMAR_NAME SQL.g)
set(JAVA_COMMAND "java")
set(ANTLR_JAR ${PROJECT_SOURCE_DIR}/third_party/antlr/antlr-3.4-complete.jar)

set(SQL_PARSER_C "${SQL_PARSER_LOCATION}/SQLParser.c")
set(SQL_LEXER_C "${SQL_PARSER_LOCATION}/SQLLexer.c")
set(SQL_PARSER_CPP "${SQL_PARSER_LOCATION}/SQLParser.cpp")
set(SQL_LEXER_CPP "${SQL_PARSER_LOCATION}/SQLLexer.cpp")

if(NOT EXISTS "${SQL_PARSER_CPP}")
add_custom_target(peloton-parser ALL 
				  COMMAND ${JAVA_COMMAND} 
				  -classpath ${ANTLR_JAR} org.antlr.Tool 
				  ${SQL_PARSER_LOCATION}/${SQL_PARSER_GRAMMAR_NAME} -o ${SQL_PARSER_LOCATION}
				  DEPENDS ${SQL_PARSER_LOCATION}/${SQL_PARSER_GRAMMAR_NAME}
)
endif()

file(RENAME ${SQL_PARSER_C} ${SQL_PARSER_CPP})
file(RENAME ${SQL_LEXER_C} ${SQL_LEXER_CPP})
file(REMOVE ${SQL_PARSER_C})
file(REMOVE ${SQL_LEXER_C})

set(ANTLR_WARNING_FLAGS " -Wno-unused-variable -Wno-unused-function -Wno-maybe-uninitialized -Wno-type-limits -Wno-address")
set_source_files_properties(${SQL_LEXER_CPP} APPEND_STRING PROPERTY COMPILE_FLAGS ${ANTLR_WARNING_FLAGS})
set_source_files_properties(${SQL_PARSER_CPP} APPEND_STRING PROPERTY COMPILE_FLAGS ${ANTLR_WARNING_FLAGS})

# --[ Proto library
file(GLOB proto_files proto/*.proto)
peloton_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python ${proto_files})

# include python files either to force generation
add_library(peloton-proto STATIC ${proto_hdrs} ${proto_srcs} ${proto_python})
set(Peloton_LINKER_LIBS peloton-proto ${Peloton_LINKER_LIBS}) # note, crucial to prepend!
peloton_default_properties(peloton-proto)

# --[ Peloton library

# creates 'srcs' lists
peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR} ${SQL_LEXER_CPP} ${SQL_PARSER_CPP})

add_library(peloton SHARED ${srcs})

target_link_libraries(peloton ${Peloton_LINKER_LIBS} peloton-proto)
add_dependencies(peloton peloton-parser)

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
