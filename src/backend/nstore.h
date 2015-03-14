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
#include "common/logger.h"

#include <iostream>
#include <string>
#include <cstdlib>

namespace nstore {

class configuration {

public:

	std::string filesystem_path;

};

}
