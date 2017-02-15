//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog_object.h
//
// Identification: src/catalog/index_catalog_object.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "catalog/abstract_catalog_object.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/printable.h"
#include "type/abstract_pool.h"
#include "type/types.h"
#include "type/value.h"
#include "catalog/schema.h"

namespace peloton {
namespace catalog {

/////////////////////////////////////////////////////////////////////
// IndexCatalogObject class definition
/////////////////////////////////////////////////////////////////////

/*
 * class IndexCatalogObject - Holds metadata of an index object
 *
 * The metadata object maintains the tuple schema and key schema of an
 * index, since the external callers does not know the actual structure of
 * the index key, so it is the index's responsibility to maintain such a
 * mapping relation and does the conversion between tuple key and index key
 */
class IndexCatalogObject : public AbstractCatalogObject {
  IndexCatalogObject() = delete;

 public:
  IndexCatalogObject(std::string index_name, oid_t index_oid, oid_t table_oid,
                     oid_t database_oid, IndexType index_type,
                     IndexConstraintType index_constraint_type,
                     const Schema *tuple_schema,
                     const Schema *key_schema,
                     const std::vector<oid_t> &key_attrs, bool unique_keys);

  ~IndexCatalogObject();

  //const std::string &GetName() const { return name_; }
  //inline oid_t GetOid() { return index_oid; }
  inline oid_t GetTableOid() { return table_oid; }
  inline oid_t GetDatabaseOid() { return database_oid; }
  inline IndexType GetIndexType() { return index_type_; }
  inline IndexConstraintType GetIndexConstraintType() { return index_constraint_type_; }

  /*
   * Returns a schema object pointer that represents
   * the schema of indexed columns, from leading column
   * to the least important columns
   */
  inline const Schema *GetKeySchema() const { return key_schema; }

  /*
   * Returns a schema object pointer that represents
   * the schema of the underlying table
   */
  inline const Schema *GetTupleSchema() const { return tuple_schema; }

  // Return the number of columns inside index key (not in tuple key)
  //
  // Note that this must be defined inside the cpp source file
  // because it uses the member of Schema which is not known here
  oid_t GetColumnCount() const;

  bool HasUniqueKeys() const { return unique_keys; }

  /*
   * GetKeyAttrs() - Returns the mapping relation between indexed columns
   *                 and base table columns
   *
   * The return value is a const vector reference
   *
   * The entry whose value is j on index i means the i-th column in the
   * key is mapped to the j-th column in the base table tuple
   */
  inline const std::vector<oid_t> &GetKeyAttrs() const { return key_attrs; }

  /*
   * GetTupleToIndexMapping() - Returns the mapping relation between tuple key
   *                            column and index key columns
   */
  inline const std::vector<oid_t> &GetTupleToIndexMapping() const {
    return tuple_attrs;
  }

  inline double GetUtility() const { return utility_ratio; }

  inline void SetUtility(double p_utility_ratio) {
    utility_ratio = p_utility_ratio;
  }

  inline bool GetVisibility() const { return (visible_); }

  inline void SetVisibility(bool visibile) { visible_ = visibile; }

  /*
   * GetInfo() - Get a string representation for debugging
   */
  const std::string GetInfo() const;

  //===--------------------------------------------------------------------===//
  // STATIC HELPERS
  //===--------------------------------------------------------------------===//

  static inline void SetDefaultVisibleFlag(bool flag) {
    LOG_DEBUG("Set IndexCatalogObject visible flag to '%s'",
              (flag ? "true" : "false"));
    index_default_visibility = flag;
  }

  ///////////////////////////////////////////////////////////////////
  // IndexCatalogObject Data Member Definition
  ///////////////////////////////////////////////////////////////////

  oid_t table_oid;
  oid_t database_oid;

  IndexType index_type_;

  IndexConstraintType index_constraint_type_;

  // schema of the indexed base table
  const Schema *tuple_schema;

  // schema of the index key which is a reordered subset
  // of the underlying table schema
  const Schema *key_schema;

 private:
  // The mapping relation between key schema and tuple schema
  std::vector<oid_t> key_attrs;

  // The mapping relation between tuple schema and key schema
  // i.e. if the column in tuple is not indexed, then it is set to INVALID_OID
  //      if the column in tuple is indexed, then it is the index in index_key
  //
  // This vector has the same length as the tuple schema.columns
  std::vector<oid_t> tuple_attrs;

  // Whether keys are unique (e.g. primary key)
  bool unique_keys;

  // utility of an index
  double utility_ratio = INVALID_RATIO;

  // If set to true, then this index is visible to the planner
  bool visible_;

  // This is a magic flag that tells us whether new
  static bool index_default_visibility;
};
}
}
