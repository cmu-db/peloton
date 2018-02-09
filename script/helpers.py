#!/usr/bin/env python3
"""Common helper functions to be used in different Python scripts."""
import distutils.spawn

def find_clangformat():
    """Finds appropriate clang-format executable."""
    #check for possible clang-format versions
    for exe in ["clang-format", "clang-format-3.6", "clang-format-3.7",
                "clang-format-3.8"
               ]:
        path = distutils.spawn.find_executable(exe)
        if not path is None:
            break
    return path
