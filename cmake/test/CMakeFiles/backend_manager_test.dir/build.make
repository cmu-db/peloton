# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.8

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CLion.app/Contents/bin/cmake/bin/cmake

# The command to remove a file.
RM = /Applications/CLion.app/Contents/bin/cmake/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/Tianyu/Desktop/peloton

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/Tianyu/Desktop/peloton/cmake

# Include any dependencies generated for this target.
include test/CMakeFiles/backend_manager_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/backend_manager_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/backend_manager_test.dir/flags.make

test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o: test/CMakeFiles/backend_manager_test.dir/flags.make
test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o: ../test/storage/backend_manager_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/Tianyu/Desktop/peloton/cmake/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o"
	cd /Users/Tianyu/Desktop/peloton/cmake/test && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o -c /Users/Tianyu/Desktop/peloton/test/storage/backend_manager_test.cpp

test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.i"
	cd /Users/Tianyu/Desktop/peloton/cmake/test && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/Tianyu/Desktop/peloton/test/storage/backend_manager_test.cpp > CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.i

test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.s"
	cd /Users/Tianyu/Desktop/peloton/cmake/test && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/Tianyu/Desktop/peloton/test/storage/backend_manager_test.cpp -o CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.s

test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o.requires:

.PHONY : test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o.requires

test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o.provides: test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o.requires
	$(MAKE) -f test/CMakeFiles/backend_manager_test.dir/build.make test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o.provides.build
.PHONY : test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o.provides

test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o.provides.build: test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o


# Object files for target backend_manager_test
backend_manager_test_OBJECTS = \
"CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o"

# External object files for target backend_manager_test
backend_manager_test_EXTERNAL_OBJECTS =

test/backend_manager_test: test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o
test/backend_manager_test: test/CMakeFiles/backend_manager_test.dir/build.make
test/backend_manager_test: lib/libpeloton-d.0.0.5.dylib
test/backend_manager_test: lib/libpeloton-test-common.a
test/backend_manager_test: lib/libpeloton-proto-d.a
test/backend_manager_test: /usr/local/lib/libboost_system-mt.dylib
test/backend_manager_test: /usr/local/lib/libboost_filesystem-mt.dylib
test/backend_manager_test: /usr/local/lib/libboost_thread-mt.dylib
test/backend_manager_test: /usr/local/lib/libgflags.dylib
test/backend_manager_test: /usr/local/lib/libprotobuf.dylib
test/backend_manager_test: /usr/local/lib/libevent.dylib
test/backend_manager_test: /usr/local/lib/libevent_pthreads.dylib
test/backend_manager_test: /usr/local/lib/libpqxx.dylib
test/backend_manager_test: /usr/local/lib/libpq.dylib
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMMCJIT.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMExecutionEngine.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMRuntimeDyld.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86AsmParser.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86CodeGen.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMAsmPrinter.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMSelectionDAG.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMCodeGen.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMTarget.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMInstrumentation.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMScalarOpts.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMInstCombine.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMProfileData.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMTransformUtils.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMipa.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMAnalysis.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86Desc.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86AsmPrinter.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86Utils.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMObject.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMBitReader.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMCore.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMMCParser.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86Disassembler.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86Info.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMMCDisassembler.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMMC.a
test/backend_manager_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMSupport.a
test/backend_manager_test: ../third_party/libpg_query/libpg_query.a
test/backend_manager_test: test/CMakeFiles/backend_manager_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/Tianyu/Desktop/peloton/cmake/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable backend_manager_test"
	cd /Users/Tianyu/Desktop/peloton/cmake/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/backend_manager_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/backend_manager_test.dir/build: test/backend_manager_test

.PHONY : test/CMakeFiles/backend_manager_test.dir/build

test/CMakeFiles/backend_manager_test.dir/requires: test/CMakeFiles/backend_manager_test.dir/storage/backend_manager_test.cpp.o.requires

.PHONY : test/CMakeFiles/backend_manager_test.dir/requires

test/CMakeFiles/backend_manager_test.dir/clean:
	cd /Users/Tianyu/Desktop/peloton/cmake/test && $(CMAKE_COMMAND) -P CMakeFiles/backend_manager_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/backend_manager_test.dir/clean

test/CMakeFiles/backend_manager_test.dir/depend:
	cd /Users/Tianyu/Desktop/peloton/cmake && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/Tianyu/Desktop/peloton /Users/Tianyu/Desktop/peloton/test /Users/Tianyu/Desktop/peloton/cmake /Users/Tianyu/Desktop/peloton/cmake/test /Users/Tianyu/Desktop/peloton/cmake/test/CMakeFiles/backend_manager_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/backend_manager_test.dir/depend

