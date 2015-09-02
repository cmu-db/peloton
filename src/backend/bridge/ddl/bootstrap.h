//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bootstrap.h
//
// Identification: src/backend/bridge/ddl/bootstrap.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/catalog/schema.h"
#include "postgres.h"
#include "c.h"
#include "raw_database_info.h"
#include "miscadmin.h"
#include "utils/rel.h"

struct peloton_status;

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Bootstrap
//===--------------------------------------------------------------------===//

class Bootstrap {

 public:

  static bool BootstrapPeloton(void);
};

}  // namespace bridge
}  // namespace peloton
