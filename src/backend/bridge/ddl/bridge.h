//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge.h
//
// Identification: src/backend/bridge/ddl/bridge.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "c.h"
#include "access/htup.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Bridge
//===--------------------------------------------------------------------===//

//  Bridge for accessing Postgres Catalog
class Bridge {
 public:
  //===--------------------------------------------------------------------===//
  // Getters
  //===--------------------------------------------------------------------===//

  static HeapTuple GetPGClassTupleForRelationOid(Oid relation_id);

  static HeapTuple GetPGClassTupleForRelationName(const char *relation_name);

  static char *GetRelationName(Oid relation_id);

  static Oid GetRelationOid(const char *relation_name);

  static int GetNumberOfAttributes(Oid relation_id);

  static float GetNumberOfTuples(Oid relation_id);

  static bool RelationExists(const char *relation_name);

  static Oid GetCurrentDatabaseOid(void);

  static void GetDatabaseList(void);

  static void GetTableList(bool catalog_only);

  //===--------------------------------------------------------------------===//
  // Setters
  //===--------------------------------------------------------------------===//

  static void SetNumberOfTuples(Oid relation_id, float num_of_tuples);

  //===--------------------------------------------------------------------===//
  // Wrapper
  //===--------------------------------------------------------------------===//

  static void PelotonStartTransactionCommand();

  static void PelotonCommitTransactionCommand();

  static void SetCurrentResourceOwner();
};

}  // namespace bridge
}  // namespace peloton
