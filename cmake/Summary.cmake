################################################################################################
# Peloton status report function.
# Automatically align right column and selects text based on condition.
# Usage:
#   peloton_status(<text>)
#   peloton_status(<heading> <value1> [<value2> ...])
#   peloton_status(<heading> <condition> THEN <text for TRUE> ELSE <text for FALSE> )
function(peloton_status text)
  set(status_cond)
  set(status_then)
  set(status_else)

  set(status_current_name "cond")
  foreach(arg ${ARGN})
    if(arg STREQUAL "THEN")
      set(status_current_name "then")
    elseif(arg STREQUAL "ELSE")
      set(status_current_name "else")
    else()
      list(APPEND status_${status_current_name} ${arg})
    endif()
  endforeach()

  if(DEFINED status_cond)
    set(status_placeholder_length 23)
    string(RANDOM LENGTH ${status_placeholder_length} ALPHABET " " status_placeholder)
    string(LENGTH "${text}" status_text_length)
    if(status_text_length LESS status_placeholder_length)
      string(SUBSTRING "${text}${status_placeholder}" 0 ${status_placeholder_length} status_text)
    elseif(DEFINED status_then OR DEFINED status_else)
      message(STATUS "${text}")
      set(status_text "${status_placeholder}")
    else()
      set(status_text "${text}")
    endif()

    if(DEFINED status_then OR DEFINED status_else)
      if(${status_cond})
        string(REPLACE ";" " " status_then "${status_then}")
        string(REGEX REPLACE "^[ \t]+" "" status_then "${status_then}")
        message(STATUS "${status_text} ${status_then}")
      else()
        string(REPLACE ";" " " status_else "${status_else}")
        string(REGEX REPLACE "^[ \t]+" "" status_else "${status_else}")
        message(STATUS "${status_text} ${status_else}")
      endif()
    else()
      string(REPLACE ";" " " status_cond "${status_cond}")
      string(REGEX REPLACE "^[ \t]+" "" status_cond "${status_cond}")
      message(STATUS "${status_text} ${status_cond}")
    endif()
  else()
    message(STATUS "${text}")
  endif()
endfunction()


################################################################################################
# Function for fetching Peloton version from git and headers
# Usage:
#   peloton_extract_peloton_version()
function(peloton_extract_peloton_version)
  set(Peloton_GIT_VERSION "unknown")
  find_package(Git)
  if(GIT_FOUND)
    execute_process(COMMAND ${GIT_EXECUTABLE} describe --tags --always --dirty
                    ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE
                    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
                    OUTPUT_VARIABLE Peloton_GIT_VERSION
                    RESULT_VARIABLE __git_result)
    if(NOT ${__git_result} EQUAL 0)
      set(Peloton_GIT_VERSION "unknown")
    endif()
  endif()

  set(Peloton_GIT_VERSION ${Peloton_GIT_VERSION} PARENT_SCOPE)
  set(Peloton_VERSION "<TODO> (Peloton doesn't declare its version in headers)" PARENT_SCOPE)

  # peloton_parse_header(${Peloton_INCLUDE_DIR}/peloton/version.hpp Peloton_VERSION_LINES PELOTON_MAJOR PELOTON_MINOR PELOTON_PATCH)
  # set(Peloton_VERSION "${PELOTON_MAJOR}.${PELOTON_MINOR}.${PELOTON_PATCH}" PARENT_SCOPE)

  # or for #define Peloton_VERSION "x.x.x"
  # peloton_parse_header_single_define(Peloton ${Peloton_INCLUDE_DIR}/peloton/version.hpp Peloton_VERSION)
  # set(Peloton_VERSION ${Peloton_VERSION_STRING} PARENT_SCOPE)

endfunction()


################################################################################################
# Prints accumulated peloton configuration summary
# Usage:
#   peloton_print_configuration_summary()

function(peloton_print_configuration_summary)
  peloton_extract_peloton_version()
  set(Peloton_VERSION ${Peloton_VERSION} PARENT_SCOPE)

  peloton_merge_flag_lists(__flags_rel CMAKE_CXX_FLAGS_RELEASE CMAKE_CXX_FLAGS)
  peloton_merge_flag_lists(__flags_deb CMAKE_CXX_FLAGS_DEBUG   CMAKE_CXX_FLAGS)

  peloton_status("")
  peloton_status("******************* Peloton Configuration Summary *******************")
  peloton_status("General:")
  peloton_status("  Version           :   ${PELOTON_TARGET_VERSION}")
  peloton_status("  Git               :   ${Peloton_GIT_VERSION}")
  peloton_status("  System            :   ${CMAKE_SYSTEM_NAME}")
  peloton_status("  Compiler          :   ${CMAKE_CXX_COMPILER} (${COMPILER_FAMILY} ${COMPILER_VERSION})")
  peloton_status("  Release CXX flags :   ${__flags_rel}")
  peloton_status("  Debug CXX flags   :   ${__flags_deb}")
  peloton_status("  Build type        :   ${CMAKE_BUILD_TYPE}")
  peloton_status("")
  peloton_status("  BUILD_docs        :   ${BUILD_docs}")
  peloton_status("")
  peloton_status("Dependencies:")
  peloton_status("  Linker flags      :   ${CMAKE_EXE_LINKER_FLAGS}")
  peloton_status("  Boost             :   Yes (ver. ${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION})")
  peloton_status("  glog              :   Yes")
  peloton_status("  gflags            :   Yes")
  peloton_status("  protobuf          : " PROTOBUF_FOUND THEN "Yes (ver. ${PROTOBUF_VERSION})" ELSE "No" )
  peloton_status("  capnproto         : " CAPNPROTO_FOUND THEN "Yes" ELSE "No" )
  peloton_status("  LLVM              :   Yes (ver. ${LLVM_PACKAGE_VERSION})" )
  peloton_status("")
  if(BUILD_docs)
    peloton_status("Documentaion:")
    peloton_status("  Doxygen           :" DOXYGEN_FOUND THEN "${DOXYGEN_EXECUTABLE} (${DOXYGEN_VERSION})" ELSE "No")
    peloton_status("  config_file       :   ${DOXYGEN_config_file}")

    peloton_status("")
  endif()
  peloton_status("Install:")
  peloton_status("  Install path      :   ${CMAKE_INSTALL_PREFIX}")
  peloton_status("")
endfunction()
