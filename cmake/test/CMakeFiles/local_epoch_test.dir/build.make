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
include test/CMakeFiles/local_epoch_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/local_epoch_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/local_epoch_test.dir/flags.make

test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o: test/CMakeFiles/local_epoch_test.dir/flags.make
test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o: ../test/concurrency/local_epoch_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/Tianyu/Desktop/peloton/cmake/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o"
	cd /Users/Tianyu/Desktop/peloton/cmake/test && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o -c /Users/Tianyu/Desktop/peloton/test/concurrency/local_epoch_test.cpp

test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.i"
	cd /Users/Tianyu/Desktop/peloton/cmake/test && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/Tianyu/Desktop/peloton/test/concurrency/local_epoch_test.cpp > CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.i

test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.s"
	cd /Users/Tianyu/Desktop/peloton/cmake/test && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/Tianyu/Desktop/peloton/test/concurrency/local_epoch_test.cpp -o CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.s

test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o.requires:

.PHONY : test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o.requires

test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o.provides: test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o.requires
	$(MAKE) -f test/CMakeFiles/local_epoch_test.dir/build.make test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o.provides.build
.PHONY : test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o.provides

test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o.provides.build: test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o


# Object files for target local_epoch_test
local_epoch_test_OBJECTS = \
"CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o"

# External object files for target local_epoch_test
local_epoch_test_EXTERNAL_OBJECTS =

test/local_epoch_test: test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o
test/local_epoch_test: test/CMakeFiles/local_epoch_test.dir/build.make
test/local_epoch_test: lib/libpeloton-d.0.0.5.dylib
test/local_epoch_test: lib/libpeloton-test-common.a
test/local_epoch_test: lib/libpeloton-proto-d.a
test/local_epoch_test: /usr/local/lib/libboost_system-mt.dylib
test/local_epoch_test: /usr/local/lib/libboost_filesystem-mt.dylib
test/local_epoch_test: /usr/local/lib/libboost_thread-mt.dylib
test/local_epoch_test: /usr/local/lib/libgflags.dylib
test/local_epoch_test: /usr/local/lib/libprotobuf.dylib
test/local_epoch_test: /usr/local/lib/libevent.dylib
test/local_epoch_test: /usr/local/lib/libevent_pthreads.dylib
test/local_epoch_test: /usr/local/lib/libpqxx.dylib
test/local_epoch_test: /usr/local/lib/libpq.dylib
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMMCJIT.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMExecutionEngine.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMRuntimeDyld.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86AsmParser.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86CodeGen.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMAsmPrinter.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMSelectionDAG.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMCodeGen.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMTarget.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMInstrumentation.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMScalarOpts.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMInstCombine.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMProfileData.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMTransformUtils.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMipa.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMAnalysis.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86Desc.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86AsmPrinter.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86Utils.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMObject.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMBitReader.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMCore.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMMCParser.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86Disassembler.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMX86Info.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMMCDisassembler.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMMC.a
test/local_epoch_test: /usr/local/Cellar/llvm@3.7/3.7.1/lib/llvm-3.7/lib/libLLVMSupport.a
test/local_epoch_test: ../third_party/libpg_query/libpg_query.a
test/local_epoch_test: test/CMakeFiles/local_epoch_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/Tianyu/Desktop/peloton/cmake/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable local_epoch_test"
	cd /Users/Tianyu/Desktop/peloton/cmake/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/local_epoch_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/local_epoch_test.dir/build: test/local_epoch_test

.PHONY : test/CMakeFiles/local_epoch_test.dir/build

test/CMakeFiles/local_epoch_test.dir/requires: test/CMakeFiles/local_epoch_test.dir/concurrency/local_epoch_test.cpp.o.requires

.PHONY : test/CMakeFiles/local_epoch_test.dir/requires

test/CMakeFiles/local_epoch_test.dir/clean:
	cd /Users/Tianyu/Desktop/peloton/cmake/test && $(CMAKE_COMMAND) -P CMakeFiles/local_epoch_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/local_epoch_test.dir/clean

test/CMakeFiles/local_epoch_test.dir/depend:
	cd /Users/Tianyu/Desktop/peloton/cmake && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/Tianyu/Desktop/peloton /Users/Tianyu/Desktop/peloton/test /Users/Tianyu/Desktop/peloton/cmake /Users/Tianyu/Desktop/peloton/cmake/test /Users/Tianyu/Desktop/peloton/cmake/test/CMakeFiles/local_epoch_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/local_epoch_test.dir/depend

