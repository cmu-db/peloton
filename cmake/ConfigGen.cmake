
################################################################################################
# Helper function to fetch peloton includes which will be passed to dependent projects
# Usage:
#   peloton_get_current_includes(<includes_list_variable>)
function(peloton_get_current_includes includes_variable)
  get_property(current_includes DIRECTORY PROPERTY INCLUDE_DIRECTORIES)
  peloton_convert_absolute_paths(current_includes)

  # remove at most one ${PROJECT_BINARY_DIR} include added for peloton_config.h
  list(FIND current_includes ${PROJECT_BINARY_DIR} __index)
  list(REMOVE_AT current_includes ${__index})

  # removing numpy includes (since not required for client libs)
  set(__toremove "")
  foreach(__i ${current_includes})
    if(${__i} MATCHES "python")
      list(APPEND __toremove ${__i})
    endif()
  endforeach()
  if(__toremove)
    list(REMOVE_ITEM current_includes ${__toremove})
  endif()

  peloton_list_unique(current_includes)
  set(${includes_variable} ${current_includes} PARENT_SCOPE)
endfunction()

################################################################################################
# Helper function to get all list items that begin with given prefix
# Usage:
#   peloton_get_items_with_prefix(<prefix> <list_variable> <output_variable>)
function(peloton_get_items_with_prefix prefix list_variable output_variable)
  set(__result "")
  foreach(__e ${${list_variable}})
    if(__e MATCHES "^${prefix}.*")
      list(APPEND __result ${__e})
    endif()
  endforeach()
  set(${output_variable} ${__result} PARENT_SCOPE)
endfunction()

################################################################################################
# Function for generation Peloton build- and install- tree export config files
# Usage:
#  peloton_generate_export_configs()
function(peloton_generate_export_configs)
  set(install_cmake_suffix "share/Peloton")

  # ---[ Configure build-tree PelotonConfig.cmake file ]---
  peloton_get_current_includes(Peloton_INCLUDE_DIRS)

  set(Peloton_DEFINITIONS "")

  configure_file("cmake/Templates/PelotonConfig.cmake.in" "${PROJECT_BINARY_DIR}/PelotonConfig.cmake" @ONLY)

  # Add targets to the build-tree export set
  export(TARGETS peloton peloton-proto FILE "${PROJECT_BINARY_DIR}/PelotonTargets.cmake")
  if (TARGET peloton-capnp)
    export(TARGETS peloton-capnp APPEND FILE "${PROJECT_BINARY_DIR}/PelotonTargets.cmake")
  endif()
  export(PACKAGE Peloton)

  # ---[ Configure install-tree PelotonConfig.cmake file ]---

  # remove source and build dir includes
  peloton_get_items_with_prefix(${PROJECT_SOURCE_DIR} Peloton_INCLUDE_DIRS __insource)
  peloton_get_items_with_prefix(${PROJECT_BINARY_DIR} Peloton_INCLUDE_DIRS __inbinary)
  list(REMOVE_ITEM Peloton_INCLUDE_DIRS ${__insource} ${__inbinary})

  # add `install` include folder
  set(lines
     "get_filename_component(__peloton_include \"\${Peloton_CMAKE_DIR}/../../include\" ABSOLUTE)\n"
     "list(APPEND Peloton_INCLUDE_DIRS \${__peloton_include})\n"
     "unset(__peloton_include)\n")
  string(REPLACE ";" "" Peloton_INSTALL_INCLUDE_DIR_APPEND_COMMAND ${lines})

  configure_file("cmake/Templates/PelotonConfig.cmake.in" "${PROJECT_BINARY_DIR}/cmake/PelotonConfig.cmake" @ONLY)

  # Install the PelotonConfig.cmake and export set to use with install-tree
  install(FILES "${PROJECT_BINARY_DIR}/cmake/PelotonConfig.cmake" DESTINATION ${install_cmake_suffix})
  # TODO: Fix the target
  #install(EXPORT PelotonTargets DESTINATION ${install_cmake_suffix})

  # ---[ Configure and install version file ]---

  # TODO: Lines below are commented because Peloton does't declare its version in headers.
  # When the declarations are added, modify `peloton_extract_peloton_version()` macro and uncomment

  # configure_file(cmake/Templates/PelotonConfigVersion.cmake.in "${PROJECT_BINARY_DIR}/PelotonConfigVersion.cmake" @ONLY)
  # install(FILES "${PROJECT_BINARY_DIR}/PelotonConfigVersion.cmake" DESTINATION ${install_cmake_suffix})
endfunction()


