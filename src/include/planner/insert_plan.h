//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_plan.h
//
// Identification: src/include/planner/insert_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/constant_value_expression.h"
#include "storage/data_table.h"
#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/project_info.h"
#include "type/abstract_pool.h"

namespace peloton {

namespace storage {
class DataTable;
class Tuple;
}

namespace parser {
class InsertStatement;
}

namespace planner {
  
class InsertPlan : public AbstractPlan {

 public:
  // Construct when SELECT comes in with it
  InsertPlan(storage::DataTable *table, oid_t bulk_insert_count = 1)
    : target_table_(table), bulk_insert_count_(bulk_insert_count) {
    LOG_TRACE("Creating an Insert Plan with SELECT as a child");
  }

  // Construct with a project info
  // This can only be handled by the interpreted exeuctor
  InsertPlan(storage::DataTable *table,
             std::unique_ptr<const planner::ProjectInfo> &&project_info,
             oid_t bulk_insert_count = 1)
    : target_table_(table), project_info_(std::move(project_info)),
      bulk_insert_count_(bulk_insert_count) {
    LOG_TRACE("Creating an Insert Plan with a projection");
  }

  // Construct with a tuple
  // This can only be handled by the interpreted exeuctor
  InsertPlan(storage::DataTable *table,
             std::unique_ptr<storage::Tuple> &&tuple,
             oid_t bulk_insert_count = 1)
    : target_table_(table), bulk_insert_count_(bulk_insert_count) {
    LOG_TRACE("Creating an Insert Plan for one tuple");
    tuples_.push_back(std::move(tuple));
  }

  /**
   * @brief Create an insert plan with specific values
   *
   * @param[in] table          Table to insert into
   * @param[in] columns        Columns to insert into
   * @param[in] insert_values  Values
   */
  InsertPlan(storage::DataTable *table,
	     const std::vector<std::string> *columns,
             const std::vector<std::vector<std::unique_ptr<expression::AbstractExpression>>> *insert_values);

  // Get a varlen pool - will construct the pool only if needed
  type::AbstractPool *GetPlanPool();

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INSERT; };

  /**
   * @brief Save values for jdbc prepared statement insert.
   *    Only a single tuple is presented to this function.
   *
   * @param[in] values  Values for insertion
   */
  void SetParameterValues(std::vector<type::Value> *values) override;

  /* 
   * Clear the parameter values of the current insert. The plan may be 
   * cached in the statement / plan cache and may be reused.
   */
  void ClearParameterValues() override { values_.clear(); }

  storage::DataTable *GetTable() const { return target_table_; }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  type::Value GetValue(uint32_t idx) const { return values_.at(idx); }

  oid_t GetBulkInsertCount() const { return bulk_insert_count_; }

  const storage::Tuple *GetTuple(int tuple_idx) const {
    if (tuple_idx >= (int)tuples_.size()) {
      return nullptr;
    }
    return tuples_[tuple_idx].get();
  }

  const std::string GetInfo() const override { return "InsertPlan"; }

  void PerformBinding(BindingContext &binding_context) override;

  const std::vector<const AttributeInfo *> &GetAttributeInfos() const {
    return ais_;
  }

  // WARNING - Not Implemented
  std::unique_ptr<AbstractPlan> Copy() const override {
    LOG_INFO("InsertPlan Copy() not implemented");
    // TODO: Add copying mechanism
    std::unique_ptr<AbstractPlan> dummy;
    return dummy;
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override {
    return !(*this == rhs);
  }

  virtual void VisitParameters(codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  // mapping from schema columns to insert columns
  struct SchemaColsToInsertCols {
    // this schema column is present in the insert columns
    bool in_insert_cols;
  
    // For a PS, insert saved value (from constant in insert values list), no
    // param value.
    bool set_value;
  
    // index of this column in insert columns values
    int  val_idx;
  
    // schema column type
    type::TypeId type;
  
    // set_value refers to this saved value
    type::Value value;
  };
  
  // Target table
  storage::DataTable *target_table_ = nullptr;

  // Values
  std::vector<type::Value> values_;

  // mapping from schema columns to vector of insert columns
  std::vector<SchemaColsToInsertCols> schema_to_insert_;
  // mapping from insert columns to schema columns
  std::vector<uint32_t> insert_to_schema_;  

  // Projection Info
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  // Tuple : To be deprecated after the interpreted execution disappears
  std::vector<std::unique_ptr<storage::Tuple>> tuples_;

  // Parameter Information <tuple_index, tuple_column_index, parameter_index>
  std::unique_ptr<std::vector<std::tuple<oid_t, oid_t, oid_t>>>
      parameter_vector_;

  // Parameter value types
  std::unique_ptr<std::vector<type::TypeId>> params_value_type_;

  // Number of times to insert
  oid_t bulk_insert_count_;

  // Vector storing attribute information for INSERT SELECT
  std::vector<const AttributeInfo *> ais_;

  // Pool for variable length types
  std::unique_ptr<type::AbstractPool> pool_;

 private:
  DISALLOW_COPY_AND_MOVE(InsertPlan);
  
  /** 
   * Lookup a column name in the schema columns
   * 
   * @param[in]  col_name    column name, from insert statement
   * @param[in]  tbl_columns table columns from the schema
   * @param[out] index       index into schema columns, only if found
   *
   * @return      true if column was found, false otherwise
   */
  bool FindSchemaColIndex(std::string col_name,
			  const std::vector<catalog::Column> &tbl_columns,
			  uint32_t &index) {
    for (auto tcol = tbl_columns.begin(); tcol != tbl_columns.end(); tcol++) {
      if (tcol->GetName() == col_name) {
	index = std::distance(tbl_columns.begin(), tcol);
	return true;
      }
    }
    return false;
  }
  
  /**
   * Process column specification supplied in the insert statement.
   * Construct a map from insert columns to schema columns. Once
   * we know which columns will receive constant inserts, further
   * adjustment of the map will be needed.
   *
   * @param[in] columns        Column specification
   */
  void ProcessColumnSpec(const std::vector<std::string> *columns) {
    auto *schema = target_table_->GetSchema();  
    auto &table_columns = schema->GetColumns();
    auto usr_col_count = columns->size();  
  
    // iterate over supplied columns
    for (size_t usr_col_id = 0; usr_col_id < usr_col_count; usr_col_id++) {
      uint32_t idx;    
      auto col_name = columns->at(usr_col_id);
    
      // determine index of column in schema
      bool found_col = FindSchemaColIndex(col_name, table_columns, idx);
      if (not found_col) {
	throw Exception("column " + col_name + " not in table " +
			target_table_->GetName() + " columns");
      }
      // we have values for this column
      schema_to_insert_[idx].in_insert_cols = true;
      // remember how to map schema col -> value for col in tuple
      schema_to_insert_[idx].val_idx = usr_col_id;
      // and the reverse
      insert_to_schema_[usr_col_id] = idx;
    }
  }  

  /**
   * Process a single expression to be inserted.
   *
   * @param[in] expr       insert expression
   * @param[in] schema_idx index into schema columns, where the expr
   *                       will be inserted.
   * @return  true if values imply a prepared statement
   *          false if all values are constants. This does not rule
   *             out the insert being a prepared statement.
   */
  bool ProcessValueExpr(expression::AbstractExpression *expr,
			uint32_t schema_idx) {
    auto type = schema_to_insert_[schema_idx].type;
  
    if (expr == nullptr) {
      SetDefaultValue(schema_idx);
    } else if (expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {

      auto *const_expr =
	dynamic_cast<expression::ConstantValueExpression *>(expr);
      type::Value value = const_expr->GetValue().CastAs(type);
    
      schema_to_insert_[schema_idx].set_value = true;
      schema_to_insert_[schema_idx].value = value;
      // save it, in case this is not a PS
      values_.push_back(value);
    
      return false;
    } else {
      PELOTON_ASSERT(expr->GetExpressionType() ==
		     ExpressionType::VALUE_PARAMETER);
      return true;
    }
    return false;
  }

  /** 
   * Set default value into a schema column
   * 
   * @param[in] idx  schema column index
   */
 void SetDefaultValue(uint32_t idx) {
   auto *schema = target_table_->GetSchema();  
   type::Value *v = schema->GetDefaultValue(idx);
   type::TypeId type = schema_to_insert_[idx].type;
  
   if (v == nullptr)
     // null default value
     values_.push_back(type::ValueFactory::GetNullValueByType(type));
   else
     // non-null default value
     values_.push_back(*v);
 }
};
}  // namespace planner
}  // namespace peloton
