#!/usr/bin/env python

import os.path
import socket
import subprocess
import sys
import time
import traceback

class RunJunit:
    """ Class to run Junit tests """
    
    def __init__(self):
        """ Locations and misc. variable initialization """
        
        # Peloton server output
        self.peloton_output_file = "/tmp/peloton_log.txt"
        # Ant Junit execution output
        self.junit_output_file = "/tmp/junit_log.txt"

        # location of Peloton, relative to this script
        self.peloton_path = "../../../build/bin/peloton"
        self.peloton_process = None

        # peloton server location
        self.peloton_host = "localhost"
        self.peloton_port = 15721
        return
    
    def _check_peloton_binary(self):
        """ Check that a Peloton binary is available """
        if not os.path.exists(self.peloton_path):
            abs_path = os.path.abspath(self.peloton_path)
            msg = "No Peloton binary found at {}".format(abs_path)
            raise RuntimeError(msg)
        return

    def _run_peloton(self):
        """ Start the Peloton server """
        self.peloton_output_fd = open(self.peloton_output_file, "w+")
        self.peloton_process = subprocess.Popen(self.peloton_path,
                                                stdout=self.peloton_output_fd,
                                                stderr=self.peloton_output_fd)
        self._wait_for_peloton()
        return

    def _wait_for_peloton(self):
        """ Wait for the peloton server to come up.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # max wait of 10s in 0.1s increments
        for i in range(100):
            try:
                s.connect((self.peloton_host, self.peloton_port))
                s.close()
                print ("connected to server in {} seconds".format(i*0.1))
                return
            except:
                time.sleep(0.1)
                continue
        return

    def _stop_peloton(self):
        """ Stop the Peloton server and print it's log file """
        # get exit code, if any 
        self.peloton_process.poll()
        if self.peloton_process.returncode is not None:
            # Peloton terminated already
            self.peloton_output_fd.close()
            self._print_output(self.peloton_output_file)            
            msg = "Peloton terminated with return code {}".format(
                self.peloton_process.returncode)
            raise RuntimeError(msg)

        # still (correctly) running, terminate it
        self.peloton_process.terminate()
        return

    def _print_output(self, filename):
        """ Print out contents of a file """
        fd = open(filename)
        lines = fd.readlines()
        for line in lines:
            print (line.strip())
        fd.close()
        return

    def _run_junit(self):
        """ Run the JUnit tests, via ant """
        self.junit_output_fd = open(self.junit_output_file, "w+")
        # use ant's junit runner, until we deprecate Ubuntu 14.04.
        # At that time switch to "ant testconsole" which invokes JUnitConsole
        # runner. It requires Java 1.8 or later, but has much cleaner
        # human readable output
        ret_val = subprocess.call(["ant test"],
                                  stdout=self.junit_output_fd,
                                  stderr=self.junit_output_fd,
                                  shell=True)
        self.junit_output_fd.close()
        return ret_val

    def run(self):
        """ Orchestrate the overall JUnit test execution """
        self._check_peloton_binary()
        self._run_peloton()
        ret_val = self._run_junit()
        self._print_output(self.junit_output_file)
        
        self._stop_peloton()
        if ret_val:
            # print the peloton log file, only if we had a failure
            self._print_output(self.peloton_output_file)     
        return ret_val

if __name__ == "__main__":
    
    # Make it so that we can invoke the script from any directory.
    # Actual execution has to be from the junit directory, so first
    # determine the absolute directory path to this script
    prog_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    # cd to the junit directory
    os.chdir(prog_dir)

    try:
        exit_code = RunJunit().run()
    except:
        print ("Exception trying to run junit tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1
        
    sys.exit(exit_code)
    
