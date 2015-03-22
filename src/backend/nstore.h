/*-------------------------------------------------------------------------
 *
 * nstore.h
 * Configuration for n-store
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/nstore.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <getopt.h>
#include <iostream>
#include <string>
#include <cstdlib>

namespace nstore {

class configuration {

public:

	std::string filesystem_path;

};

// sample test helper
int SampleFunc(int a, int b) {
	return a+b;
}

}
