if (NOT __CAPNP_INCLUDED) # guard against multiple includes
  set(__CAPNP_INCLUDED TRUE)

  # use the system-wide capnp if present
  find_package(CapnProto 0.6.1 QUIET)
  if (CAPNP_FOUND)
    set(CAPNP_EXTERNAL FALSE)
  else()
    # capnp will use pthreads if it's available in the system, so we must link with it
    find_package(Threads)

    # build directory
    set(capnp_PREFIX ${CMAKE_BINARY_DIR}/external/capnp-prefix)
    # install directory
    set(capnp_INSTALL ${CMAKE_BINARY_DIR}/external/capnp-install)

    # we build capnp statically, but want to link it into the peloton shared library
    # this requires position-independent code
    if (UNIX)
        set(CAPNP_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(CAPNP_CXX_FLAGS ${CMAKE_CXX_FLAGS} ${CAPNP_EXTRA_COMPILER_FLAGS})
    set(CAPNP_C_FLAGS ${CMAKE_C_FLAGS} ${CAPNP_EXTRA_COMPILER_FLAGS})

    ExternalProject_Add(capnp
      PREFIX ${capnp_PREFIX}
      DOWNLOAD_COMMAND ""
      SOURCE_DIR ${PROJECT_SOURCE_DIR}/third_party/capnproto/c++
      UPDATE_COMMAND ""
      INSTALL_DIR ${capnp_INSTALL}
      CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                 -DCMAKE_INSTALL_PREFIX=${capnp_INSTALL}
                 -DBUILD_SHARED_LIBS=OFF
                 -DBUILD_STATIC_LIBS=ON
                 -DBUILD_PACKAGING=OFF
                 -DBUILD_TESTING=OFF
                 -DBUILD_NC_TESTS=OFF
                 -BUILD_CONFIG_TESTS=OFF
                 -DINSTALL_HEADERS=ON
                 -DCMAKE_C_FLAGS=${CAPNP_C_FLAGS}
                 -DCMAKE_CXX_FLAGS=${CAPNP_CXX_FLAGS}
      LOG_DOWNLOAD 1
      LOG_INSTALL 1
      )

    set(CAPNP_FOUND TRUE)
    set(CAPNP_EXECUTABLE ${capnp_INSTALL}/bin/capnp)
    set(CAPNPC_CXX_EXECUTABLE ${capnp_INSTALL}/bin/capnpc-c++)
    set(CAPNP_INCLUDE_DIRS ${capnp_INSTALL}/include)
    set(CAPNP_INCLUDE_DIRECTORY ${capnp_INSTALL}/include)
    set(CAPNP_LIBRARIES
      ${capnp_INSTALL}/lib/libcapnp-rpc.a
      ${capnp_INSTALL}/lib/libcapnp.a
      ${capnp_INSTALL}/lib/libkj-async.a
      ${capnp_INSTALL}/lib/libkj.a
      ${CMAKE_THREAD_LIBS_INIT}
      )
    set(CAPNP_LIBRARY_DIRS ${capnp_INSTALL}/lib)
    set(CAPNP_EXTERNAL TRUE)

    list(APPEND external_project_dependencies capnp)

  endif()

endif()
