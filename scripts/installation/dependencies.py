#!/usr/bin/env python
# encoding: utf-8

## ==============================================
## GOAL : Install Dependencies
## ==============================================

import sys
import shlex
import shutil
import tempfile
import os
import time
import logging
import argparse
import pprint
import numpy
import re
import fnmatch
import string
import subprocess
import tempfile

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## CONFIGURATION
## ==============================================

my_env = os.environ.copy()
FILE_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.join(os.path.dirname(FILE_DIR), os.pardir)
THIRD_PARTY_DIR = os.path.join(ROOT_DIR, "third_party")

NVML_DIR = os.path.join(THIRD_PARTY_DIR, "nvml")
NANOMSG_DIR = os.path.join(THIRD_PARTY_DIR, "nanomsg")
LOGCABIN_DIR = os.path.join(THIRD_PARTY_DIR, "logcabin-1.1.0")

LOGCABIN_OBJ = 'build/Server/Main.o  \
build/Server/ClientService.o  \
build/Server/ControlService.o  \
build/Server/Globals.o  \
build/Server/RaftConsensus.o  \
build/Server/RaftConsensusInvariants.o  \
build/Server/RaftService.o  \
build/Server/ServerStats.o  \
build/Server/StateMachine.o  \
build/Server/SnapshotMetadata.pb.o  \
build/Server/SnapshotStateMachine.pb.o  \
build/Server/SnapshotStats.pb.o  \
build/Storage/FilesystemUtil.o  \
build/Storage/Layout.o  \
build/Storage/Log.o  \
build/Storage/LogFactory.o  \
build/Storage/MemoryLog.o  \
build/Storage/SegmentedLog.o  \
build/Storage/SimpleFileLog.o  \
build/Storage/SnapshotFile.o  \
build/Storage/SegmentedLog.pb.o  \
build/Storage/SimpleFileLog.pb.o  \
build/Tree/ProtoBuf.o  \
build/Tree/Tree.o  \
build/Tree/Snapshot.pb.o  \
build/Client/Backoff.o  \
build/Client/Client.o  \
build/Client/ClientImpl.o  \
build/Client/LeaderRPC.o  \
build/Client/MockClientImpl.o  \
build/Client/SessionManager.o  \
build/Client/Util.o  \
build/Protocol/Client.pb.o  \
build/Protocol/Raft.pb.o  \
build/Protocol/RaftLogMetadata.pb.o  \
build/Protocol/ServerControl.pb.o  \
build/Protocol/ServerStats.pb.o  \
build/RPC/Address.o  \
build/RPC/ClientRPC.o  \
build/RPC/ClientSession.o  \
build/RPC/MessageSocket.o  \
build/RPC/OpaqueClientRPC.o  \
build/RPC/OpaqueServer.o  \
build/RPC/OpaqueServerRPC.o  \
build/RPC/Protocol.o  \
build/RPC/Server.o  \
build/RPC/ServerRPC.o  \
build/RPC/ThreadDispatchService.o  \
build/Event/File.o  \
build/Event/Loop.o  \
build/Event/Signal.o  \
build/Event/Timer.o  \
build/Core/Buffer.o  \
build/Core/Checksum.o  \
build/Core/ConditionVariable.o  \
build/Core/Config.o  \
build/Core/Debug.o  \
build/Core/ProtoBuf.o  \
build/Core/Random.o  \
build/Core/RollingStat.o  \
build/Core/ThreadId.o  \
build/Core/Time.o  \
build/Core/StringUtil.o  \
build/Core/Util.o  \
build/Core/ProtoBufTest.pb.o'

LOGCABIN_MAIN_OBJ = 'g++ -o build/Server/Main.o -c \
-std=c++11 -fno-strict-overflow \
-fPIC -Wall -Wextra -Wcast-align \
-Wcast-qual -Wconversion -Weffc++ \
-Wformat=2 -Wmissing-format-attribute \
-Wno-non-template-friend -Wno-unused-parameter \
-Woverloaded-virtual -Wwrite-strings -DSWIG -g \
-DDEBUG -I. -Iinclude build/Server/Main.cc'

## ==============================================
## Utilities
## ==============================================

def exec_cmd(cmd):
    """
    Execute the external command and get its exitcode, stdout and stderr.
    """
    args = shlex.split(cmd)
    verbose = True

    # TRY
    FNULL = open(os.devnull, 'w')
    try:
        if verbose == True:
            subprocess.check_call(args, env=my_env)
        else:
            subprocess.check_call(args, stdout=FNULL, stderr=subprocess.STDOUT, env=my_env)
    # Exception
    except subprocess.CalledProcessError as e:
        print "Command     :: ", e.cmd
        print "Return Code :: ", e.returncode
        print "Output      :: ", e.output
    # Finally
    finally:
        FNULL.close()

def install_dependencies():

    LOG.info(os.getcwd())
    LOG.info(FILE_DIR)
    LOG.info(ROOT_DIR)

    ## ==============================================
    ## NVM Library
    ## ==============================================
    LOG.info(NVML_DIR)
    LOG.info("Installing nvml library")
    os.chdir(NVML_DIR)
    cmd = 'make -j4'
    exec_cmd(cmd)
    cmd = 'sudo make install -j4'
    exec_cmd(cmd)
    os.chdir('..')

    LOG.info("Finished installing nvml library")

    ## ==============================================
    ## Nanomsg Library
    ## ==============================================
    LOG.info(NANOMSG_DIR)
    LOG.info("Installing nanomsg library")
    os.chdir(NANOMSG_DIR)
    cmd = './configure'
    exec_cmd(cmd)
    cmd = 'make -j4'
    exec_cmd(cmd)
    cmd = 'sudo make install -j4'
    exec_cmd(cmd)
    os.chdir('..')

    LOG.info("Finished installing nanomsg library")

    ## ==============================================
    ## LogCabin Library
    ## ==============================================
    LOG.info(LOGCABIN_DIR)
    LOG.info("Installing LogCabin library")
    os.chdir(LOGCABIN_DIR)
    cmd = 'scons'
    exec_cmd(cmd)
    LOG.info("LogCabin build finished")

    # Change Main.cc main function
    #cmd = 'sed -i s/main\(/logcabin_main\(/ build/Server/Main.cc'
    cmd = 'cp ./build/Server/Main.cc ./build/Server/Main.cc.bak'
    exec_cmd(cmd)
    cmd = 'cp ./Server/Main.logcabin ./build/Server/Main.cc'
    exec_cmd(cmd)
    LOG.info("Replaced main with logcabin_main")

    # Re-compile Main.cc
    cmd = LOGCABIN_MAIN_OBJ
    exec_cmd(cmd)
    LOG.info("Recompiled Main.cc")

    # Make archive here
    cmd = 'ar rcs libraft.a ' + LOGCABIN_OBJ
    exec_cmd(cmd)
    LOG.info("Created Archive")
    # Just need to link it with these flags while compiling and using it: -lpthread -lprotobuf -lrt -lcryptopp

    os.chdir('..')

    LOG.info("Finished installing logcabin library")

## ==============================================
## MAIN
## ==============================================
## MAIN
## ==============================================
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Install Dependencies')

    args = parser.parse_args()

    try:
        prev_dir = os.getcwd()

        install_dependencies()

    finally:
        # Go back to prev dir
        os.chdir(prev_dir)

