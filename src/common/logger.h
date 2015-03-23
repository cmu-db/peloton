/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "easylogging/easylogging++.h"

// Disable logging in production
#ifdef NDEBUG
#define ELPP_DISABLE_LOGS
#endif

