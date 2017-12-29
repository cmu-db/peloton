//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_plan.cpp
//
// Identification: src/planner/seq_scan_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/seq_scan_plan.h"

#include "parser/select_statement.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/macros.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "common/internal_types.h"

namespace peloton {
namespace planner {

//===--------------------------------------------------------------------===//
// Serialization/Deserialization
//===--------------------------------------------------------------------===//

/**
 * The SeqScanPlan has the following members:
 *   database_id, table_id, predicate, column_id, parent(might be NULL)
 * TODO: SeqScanPlan doesn't have children, so we don't need to handle it
 *
 * Therefore a SeqScanPlan is serialized as:
 * [(int) total size]
 * [(int8_t) plan type]
 * [(int) database_id]
 * [(int) table_id]
 * [(int) num column_id]
 * [(int) column id...]
 * [(int8_t) expr type]     : if invalid, predicate is null
 * [(bytes) predicate]      : predicate is Expression
 * [(int8_t) plan type]     : if invalid, parent is null
 * [(bytes) parent]         : parent is also a plan
 *
 * TODO: parent_ seems never be set or used
 */

bool SeqScanPlan::SerializeTo(SerializeOutput &output) const {
  // A placeholder for the total size written at the end
  int start = output.Position();
  output.WriteInt(-1);

  // Write the SeqScanPlan type
  PlanNodeType plan_type = GetPlanNodeType();
  output.WriteByte(static_cast<int8_t>(plan_type));

  // Write database id and table id
  if (!GetTable()) {
    // The plan is not completed
    return false;
  }
  oid_t database_id = GetTable()->GetDatabaseOid();
  oid_t table_id = GetTable()->GetOid();

  output.WriteInt(static_cast<int>(database_id));
  output.WriteInt(static_cast<int>(table_id));

  // If column has 0 item, just write the columnid_count with 0
  int columnid_count = GetColumnIds().size();
  output.WriteInt(columnid_count);

  // If column has 0 item, nothing happens here
  for (int it = 0; it < columnid_count; it++) {
    oid_t col_id = GetColumnIds()[it];
    output.WriteInt(static_cast<int>(col_id));
  }

  // Write predicate
  if (GetPredicate() == nullptr) {
    // Write the type
    output.WriteByte(static_cast<int8_t>(ExpressionType::INVALID));
  } else {
    // Write the expression type
    ExpressionType expr_type = GetPredicate()->GetExpressionType();
    output.WriteByte(static_cast<int8_t>(expr_type));
  }

  // Write parent, but parent seems never be set or used right now
  if (GetParent() == nullptr) {
    // Write the type
    output.WriteByte(static_cast<int8_t>(PlanNodeType::INVALID));
  } else {
    // Write the parent type
    PlanNodeType parent_type = GetParent()->GetPlanNodeType();
    output.WriteByte(static_cast<int8_t>(parent_type));

    // Write parent
    GetParent()->SerializeTo(output);
  }

  // Write the total length
  int32_t sz = static_cast<int32_t>(output.Position() - start - sizeof(int));
  PL_ASSERT(sz > 0);
  output.WriteIntAt(start, sz);

  return true;
}

/**
   * Therefore a SeqScanPlan is serialized as:
   * [(int) total size]
   * [(int8_t) plan type]
   * [(int) database_id]
   * [(int) table_id]
   * [(int) num column_id]
   * [(int) column id...]
   * [(int8_t) expr type]     : if invalid, predicate is null
   * [(bytes) predicate]      : predicate is Expression
   * [(int8_t) plan type]     : if invalid, parent is null
   * [(bytes) parent]         : parent is also a plan
 */
bool SeqScanPlan::DeserializeFrom(SerializeInput &input) {
  // Read the size of SeqScanPlan class
  input.ReadInt();

  // Read the type
  UNUSED_ATTRIBUTE PlanNodeType plan_type =
      (PlanNodeType)input.ReadEnumInSingleByte();
  PL_ASSERT(plan_type == GetPlanNodeType());

  // Read database id
  oid_t database_oid = input.ReadInt();

  // Read table id
  oid_t table_oid = input.ReadInt();

  // Get table and set it to the member
  storage::DataTable *target_table = nullptr;
  try{
      target_table = static_cast<storage::DataTable *>(
        storage::StorageManager::GetInstance()->GetTableWithOid(
              database_oid, table_oid));
  } catch (CatalogException &e) {
      LOG_TRACE("Can't find table %d! Return false", table_oid);
      return false;
  }
  SetTargetTable(target_table);

  // Read the number of column_id and set them to column_ids_
  oid_t columnid_count = input.ReadInt();
  for (oid_t it = 0; it < columnid_count; it++) {
    oid_t column_id = input.ReadInt();
    AddColumnId(column_id);
  }

  // Read the type
  ExpressionType expr_type = (ExpressionType)input.ReadEnumInSingleByte();

  // Predicate deserialization
  if (expr_type != ExpressionType::INVALID) {
    switch (expr_type) {
      //            case ExpressionType::COMPARE_IN:
      //                predicate_ =
      //                std::unique_ptr<ExpressionType::COMPARE_IN>(new
      //                ComparisonExpression (101));
      //                predicate_.DeserializeFrom(input);
      //              break;

      default: {
        LOG_ERROR(
            "Expression deserialization :: Unsupported EXPRESSION_TYPE: %s",
            ExpressionTypeToString(expr_type).c_str());
        break;
      }
    }
  }

  // Read the type of parent
  PlanNodeType parent_type = (PlanNodeType)input.ReadEnumInSingleByte();

  // Parent deserialization
  if (parent_type != PlanNodeType::INVALID) {
    switch (expr_type) {
      //            case ExpressionType::COMPARE_IN:
      //                predicate_ =
      //                std::unique_ptr<ExpressionType::COMPARE_IN>(new
      //                ComparisonExpression (101));
      //                predicate_.DeserializeFrom(input);
      //              break;

      default: {
        LOG_ERROR("Parent deserialization :: Unsupported PlanNodeType: %s",
                  ExpressionTypeToString(expr_type).c_str());
        break;
      }
    }
  }

  return true;
}
/**
 *
 * SeqScanPlan is serialized as:
 * [(int) total size]
 * [(int8_t) plan type]
 * [(int) database_id]
 * [(int) table_id]
 * [(int) num column_id]
 * [(int) column id...]
 * [(int8_t) expr type]     : if invalid, predicate is null
 * [(bytes) predicate]      : predicate is Expression
 * [(int8_t) plan type]     : if invalid, parent is null
 * [(bytes) parent]         : parent is also a plan
 *
 * So, the fixed size part is:
 *      [(int) total size]   4 +
 *      [(int8_t) plan type] 1 +
 *      [(int) database_id]  4 +
 *      [(int) table_id]     4 +
 *      [(int) num column_id]4 +
 *      [(int8_t) expr type] 1 +
 *      [(int8_t) plan type] 1 =
 *     the variant part is :
 *      [(int) column id...]: num column_id * 4
 *      [(bytes) predicate] : predicate->GetSerializeSize()
 *      [(bytes) parent]    : parent->GetSerializeSize()
 */
int SeqScanPlan::SerializeSize() const {
  // Fixed size. see the detail above
  int size_fix = sizeof(int) * 4 + 3;
  int size_column_ids = GetColumnIds().size() * sizeof(int);
  int size = size_fix + size_column_ids;

  if (GetPredicate() != nullptr) {
    size = size + GetPredicate()->SerializeSize();
  }
  if (Parent()) {
    size = size + Parent()->SerializeSize();
  }

  return size;
}

oid_t SeqScanPlan::GetColumnID(std::string col_name) {
  auto &columns = GetTable()->GetSchema()->GetColumns();
  oid_t index = -1;
  for (oid_t i = 0; i < columns.size(); ++i) {
    if (columns[i].GetName() == col_name) {
      index = i;
      break;
    }
  }
  return index;
}

void SeqScanPlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Sequential Scan");

  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
}

hash_t SeqScanPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  hash = HashUtil::CombineHashes(hash, GetTable()->Hash());
  if (GetPredicate() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  for (auto &column_id : GetColumnIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&column_id));
  }

  auto is_update = IsForUpdate();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_update));

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool SeqScanPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::SeqScanPlan &>(rhs);
  auto *table = GetTable();
  auto *other_table = other.GetTable();
  PL_ASSERT(table && other_table);
  if (*table != *other_table)
    return false;

  // Predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) ||
      (pred != nullptr && other_pred == nullptr))
    return false;
  if (pred && *pred != *other_pred)
    return false;

  // Column Ids
  size_t column_id_count = GetColumnIds().size();
  if (column_id_count != other.GetColumnIds().size())
    return false;
  for (size_t i = 0; i < column_id_count; i++) {
    if (GetColumnIds()[i] != other.GetColumnIds()[i]) {
      return false;
    }
  }

  if (IsForUpdate() != other.IsForUpdate())
    return false;

  return AbstractPlan::operator==(rhs);
}


void SeqScanPlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  auto *predicate =
      const_cast<expression::AbstractExpression *>(GetPredicate());
  if (predicate != nullptr) {
    predicate->VisitParameters(map, values, values_from_user);
  }
}

}  // namespace planner
}  // namespace peloton
