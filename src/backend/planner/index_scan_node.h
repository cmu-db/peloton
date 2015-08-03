/**
 * @brief Header for index scan plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_scan_node.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class Tuple;
}

namespace planner {

class IndexScanNode : public AbstractScanNode {
 public:
  IndexScanNode(const IndexScanNode &) = delete;
  IndexScanNode &operator=(const IndexScanNode &) = delete;
  IndexScanNode(IndexScanNode &&) = delete;
  IndexScanNode &operator=(IndexScanNode &&) = delete;

  struct IndexScanDesc {
    index::Index *index;
    std::vector<oid_t> key_ids;
    std::vector<ExpressionType> compare_types;
    std::vector<Value> keys;

    IndexScanDesc()
        : index(nullptr) {}
  };

  IndexScanNode(expression::AbstractExpression *predicate,
                const std::vector<oid_t> &column_ids,
                storage::AbstractTable *table,
                const IndexScanDesc &index_scan_desc)
      : AbstractScanNode(predicate, column_ids),
        table_(table),
        index_(index_scan_desc.index),
        key_ids_(std::move(index_scan_desc.key_ids)),
        compare_types_(std::move(index_scan_desc.compare_types)),
        keys_(std::move(index_scan_desc.keys)) {}

  ~IndexScanNode() {}

  const storage::AbstractTable *GetTable() const { return table_; }

  index::Index *GetIndex() const { return index_; }

  const std::vector<oid_t> &GetKeyIds() const { return key_ids_; }

  const std::vector<ExpressionType> &GetCompareTypes() const { return compare_types_; }

  const std::vector<Value> &GetKeys() const { return keys_; }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_INDEXSCAN;
  }

  inline std::string GetInfo() const { return "IndexScan"; }

 private:
  /** @brief Pointer to table to scan from. */
  const storage::AbstractTable *table_;

  /** @brief index associated with index scan. */
  index::Index *index_;

  const std::vector<oid_t> key_ids_;
  const std::vector<ExpressionType> compare_types_;
  const std::vector<Value> keys_;




};

}  // namespace planner
}  // namespace peloton
