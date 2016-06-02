################################################################################################
# Defines global Peloton_LINK flag, This flag is required to prevent linker from excluding
# some objects which are not addressed directly but are registered via static constructors
macro(peloton_set_peloton_link)
  if(BUILD_SHARED_LIBS)
    set(Peloton_LINK peloton)
  else()
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
      set(Peloton_LINK -Wl,-force_load peloton)
    elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
      set(Peloton_LINK -Wl,--whole-archive peloton -Wl,--no-whole-archive)
    endif()
  endif()
endmacro()
################################################################################################
# Convenient command to setup source group for IDEs that support this feature (VS, XCode)
# Usage:
#   peloton_source_group(<group> GLOB[_RECURSE] <globbing_expression>)
function(peloton_source_group group)
  cmake_parse_arguments(PELOTON_SOURCE_GROUP "" "" "GLOB;GLOB_RECURSE" ${ARGN})
  if(PELOTON_SOURCE_GROUP_GLOB)
    file(GLOB srcs1 ${PELOTON_SOURCE_GROUP_GLOB})
    source_group(${group} FILES ${srcs1})
  endif()

  if(PELOTON_SOURCE_GROUP_GLOB_RECURSE)
    file(GLOB_RECURSE srcs2 ${PELOTON_SOURCE_GROUP_GLOB_RECURSE})
    source_group(${group} FILES ${srcs2})
  endif()
endfunction()

################################################################################################
# Collecting sources from globbing and appending to output list variable
# Usage:
#   peloton_collect_sources(<output_variable> GLOB[_RECURSE] <globbing_expression>)
function(peloton_collect_sources variable)
  cmake_parse_arguments(PELOTON_COLLECT_SOURCES "" "" "GLOB;GLOB_RECURSE" ${ARGN})
  if(PELOTON_COLLECT_SOURCES_GLOB)
    file(GLOB srcs1 ${PELOTON_COLLECT_SOURCES_GLOB})
    set(${variable} ${variable} ${srcs1})
  endif()

  if(PELOTON_COLLECT_SOURCES_GLOB_RECURSE)
    file(GLOB_RECURSE srcs2 ${PELOTON_COLLECT_SOURCES_GLOB_RECURSE})
    set(${variable} ${variable} ${srcs2})
  endif()
endfunction()

################################################################################################
# Short command getting peloton sources (assuming standard Peloton code tree)
# Usage:
#   peloton_pickup_peloton_sources(<root>)
function(peloton_pickup_peloton_sources root)
  # put all files in source groups (visible as subfolder in many IDEs)
  peloton_source_group("Include"        GLOB "${root}/src/include/*/*.h")
  peloton_source_group("Include"        GLOB "${PROJECT_BINARY_DIR}/peloton_config.h*")
  peloton_source_group("Source"         GLOB "${root}/src/*/*.cpp")
  peloton_source_group("Source\\Proto"  GLOB "${root}/src/proto/*.proto")

  # source groups for test target
  peloton_source_group("Include"       GLOB "${root}/test/include/*/*.h")
  peloton_source_group("Source"       GLOB "${root}/test/*/*.cpp")

  # collect files
  file(GLOB test_hdrs    ${root}/test/include/*/*.h*)
  file(GLOB test_srcs    ${root}/test/*/*.cpp)
  file(GLOB_RECURSE hdrs ${root}/include/*/*.h*)
  file(GLOB_RECURSE srcs ${root}/src/*/*.cpp)
  list(REMOVE_ITEM  hdrs ${test_hdrs})
  list(REMOVE_ITEM  srcs ${test_srcs})

  # adding headers to make the visible in some IDEs (Qt, VS, Xcode)
  list(APPEND srcs ${hdrs} ${PROJECT_BINARY_DIR}/peloton_config.h)
  list(APPEND test_srcs ${test_hdrs})

  # add proto to make them editable in IDEs too
  file(GLOB_RECURSE proto_files ${root}/src/peloton/*.proto)
  list(APPEND srcs ${proto_files})

  # convert to absolute paths
  peloton_convert_absolute_paths(srcs)
  peloton_convert_absolute_paths(test_srcs)

  # propogate to parent scope
  set(srcs ${srcs} PARENT_SCOPE)
endfunction()

################################################################################################
# Short command for setting defeault target properties
# Usage:
#   peloton_default_properties(<target>)
function(peloton_default_properties target)
  set_target_properties(${target} PROPERTIES
    DEBUG_POSTFIX ${Peloton_DEBUG_POSTFIX}
    ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/lib"
    LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/lib"
    RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")
  # make sure we build all external depepdencies first
  if (DEFINED external_project_dependencies)
    add_dependencies(${target} ${external_project_dependencies})
  endif()
endfunction()

################################################################################################
# Short command for setting runtime directory for build target
# Usage:
#   peloton_set_runtime_directory(<target> <dir>)
function(peloton_set_runtime_directory target dir)
  set_target_properties(${target} PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${dir}")
endfunction()

################################################################################################
# Short command for setting solution folder property for target
# Usage:
#   peloton_set_solution_folder(<target> <folder>)
function(peloton_set_solution_folder target folder)
  if(USE_PROJECT_FOLDERS)
    set_target_properties(${target} PROPERTIES FOLDER "${folder}")
  endif()
endfunction()

################################################################################################
# Reads lines from input file, prepends source directory to each line and writes to output file
# Usage:
#   peloton_configure_testdatafile(<testdatafile>)
function(peloton_configure_testdatafile file)
  file(STRINGS ${file} __lines)
  set(result "")
  foreach(line ${__lines})
    set(result "${result}${PROJECT_SOURCE_DIR}/${line}\n")
  endforeach()
  file(WRITE ${file}.gen.cmake ${result})
endfunction()

################################################################################################
# Filter out all files that are not included in selected list
# Usage:
#   peloton_leave_only_selected_tests(<filelist_variable> <selected_list>)
function(peloton_leave_only_selected_tests file_list)
  if(NOT ARGN)
    return() # blank list means leave all
  endif()
  string(REPLACE "," ";" __selected ${ARGN})
  list(APPEND __selected peloton_main)

  set(result "")
  foreach(f ${${file_list}})
    get_filename_component(name ${f} NAME_WE)
    string(REGEX REPLACE "^test_" "" name ${name})
    list(FIND __selected ${name} __index)
    if(NOT __index EQUAL -1)
      list(APPEND result ${f})
    endif()
  endforeach()
  set(${file_list} ${result} PARENT_SCOPE)
endfunction()

