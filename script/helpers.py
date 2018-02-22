#!/usr/bin/env python3
"""Common helper functions to be used in different Python scripts."""
import difflib
import distutils.spawn
import logging
import os
import subprocess

from functools import reduce

CODE_SOURCE_DIR = os.path.abspath(os.path.dirname(__file__))
PELOTON_DIR = CODE_SOURCE_DIR.replace('/script', '')
CLANG_FORMAT_FILE = os.path.join(PELOTON_DIR, ".clang-format")

FORMATTING_FILE_WHITELIST = [
    # Fill me
]

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_HANDLER = logging.StreamHandler()
LOG_FORMATTER = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_HANDLER.setFormatter(LOG_FORMATTER)
LOG.addHandler(LOG_HANDLER)
LOG.setLevel(logging.INFO)

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

CLANG_FORMAT = find_clangformat()
CLANG_COMMAND_PREFIX = [CLANG_FORMAT, "-style=file"]

def clang_check(file_path):
    """Checks and reports bad code formatting."""
    rel_path_from_peloton_dir = os.path.relpath(file_path, PELOTON_DIR)

    if rel_path_from_peloton_dir in FORMATTING_FILE_WHITELIST:
        return True

    file_status = True

    # Run clang-format on the file
    if CLANG_FORMAT is None:
        LOG.error("clang-format seems not installed")
        exit()
    clang_format_cmd = CLANG_COMMAND_PREFIX + [file_path]
    formatted_src = subprocess.check_output(clang_format_cmd).splitlines(True)

    # For Python 3, the above command gives a list of binary sequences, each
    # of which has to be converted to string for diff to operate correctly.
    # Otherwise, strings would be compared with binary sequences and there
    # will always be a big difference.
    formatted_src = [line.decode('utf-8') for line in formatted_src]
    # Load source file
    with open(file_path, "r") as file:
        src = file.readlines()

        # Do the diff
        difference = difflib.Differ()
        diff = difference.compare(src, formatted_src)
        line_num = 0
        for line in diff:
            code = line[:2]
            if code in ("  ", "- "):
                line_num += 1
            if code == '- ':
                if file_status:
                    LOG.info("Invalid formatting in file : " + file_path)
                LOG.info("Line %d: %s", line_num, line[2:].strip())
                file_status = False

        return file_status


def clang_format(file_path):
    """Formats the file at file_path"""
    if CLANG_FORMAT is None:
        LOG.error("clang-format seems not installed")
        exit()

    formatting_command = CLANG_COMMAND_PREFIX + ["-i", file_path]
    LOG.info(' '.join(formatting_command))
    subprocess.call(formatting_command)
