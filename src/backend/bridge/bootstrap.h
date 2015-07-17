/*-------------------------------------------------------------------------
 *
 * bootstrap.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/bootstrap.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "postgres.h"
#include "c.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "utils/rel.h"

#include "backend/catalog/schema.h"
#include "backend/bridge/ddl.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Bootstrap
//===--------------------------------------------------------------------===//

//  Boostrap the peloton system based on Postgres Catalog
class Bootstrap {

 public:

  /**
   * @brief This function constructs all the user-defined tables and indices in all databases
   * @return true or false, depending on whether we could bootstrap.
   */
  static bool BootstrapPeloton(void);

 private:

  // Transform a pg class tuple to a list of columns
  static std::vector<peloton::catalog::Column>
  GetRelationColumns(Oid tuple_oid, Relation pg_attribute_rel);

  // Create a peloton table or index
  static void CreatePelotonStructure(char relation_kind,
                              char *relation_name,
                              Oid tuple_oid,
                              const std::vector<catalog::Column>& columns,
                              std::vector<bridge::DDL::IndexInfo>& index_infos);

  // Set up the foreign keys constraints
  static void CreateIndexInfos(oid_t tuple_oid, 
                               char* relation_name,
                               const std::vector<catalog::Column>& columns,
                               std::vector<bridge::DDL::IndexInfo>& index_infos);

  // Set up the foreign keys constraints
  static void LinkForeignKeys();

};

} // namespace bridge
} // namespace peloton
