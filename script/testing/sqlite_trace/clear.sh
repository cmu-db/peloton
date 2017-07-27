#!/bin/bash

if [ -d peloton-test ]; then rm -rf peloton-test; fi;

if [ -d sqllite ]; then rm -rf sqllite; fi;

if [ -d out ]; then rm -rf out; fi;

if [ -f config.pyc ]; then rm -f config.pyc; fi;
