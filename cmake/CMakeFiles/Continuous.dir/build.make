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

# Utility rule file for Continuous.

# Include the progress variables for this target.
include CMakeFiles/Continuous.dir/progress.make

CMakeFiles/Continuous:
	/Applications/CLion.app/Contents/bin/cmake/bin/ctest -C . -D Continuous

Continuous: CMakeFiles/Continuous
Continuous: CMakeFiles/Continuous.dir/build.make

.PHONY : Continuous

# Rule to build all files generated by this target.
CMakeFiles/Continuous.dir/build: Continuous

.PHONY : CMakeFiles/Continuous.dir/build

CMakeFiles/Continuous.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/Continuous.dir/cmake_clean.cmake
.PHONY : CMakeFiles/Continuous.dir/clean

CMakeFiles/Continuous.dir/depend:
	cd /Users/Tianyu/Desktop/peloton/cmake && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/Tianyu/Desktop/peloton /Users/Tianyu/Desktop/peloton /Users/Tianyu/Desktop/peloton/cmake /Users/Tianyu/Desktop/peloton/cmake /Users/Tianyu/Desktop/peloton/cmake/CMakeFiles/Continuous.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/Continuous.dir/depend

