//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pg_config.h
//
// Identification: src/include/parser/pg_config.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#ifdef __APPLE__
#include "parser/port_config/darwin/pg_config.h"
#else
#include "parser/port_config/linux/pg_config.h"
#endif
