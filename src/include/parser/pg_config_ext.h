//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pg_config_ext.h
//
// Identification: src/include/parser/pg_config_ext.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#ifdef __APPLE__
#include "parser/port_config/darwin/pg_config_ext.h"
#else
#include "parser/port_config/linux/pg_config_ext.h"
#endif

