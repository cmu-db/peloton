/**
 * @brief Header for delete plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/planner/abstract_plan_node.h"

#include "backend/expression/abstract_expression.h"
#include "backend/common/types.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace planner {

class UpdateNode : public AbstractPlanNode {
public:
    UpdateNode() = delete;
    UpdateNode(const UpdateNode &) = delete;
    UpdateNode& operator=(const UpdateNode &) = delete;
    UpdateNode(UpdateNode &&) = delete;
    UpdateNode& operator=(UpdateNode &&) = delete;

    typedef std::vector<std::pair<oid_t, expression::AbstractExpression*>> ColumnExprs;

    explicit UpdateNode(storage::DataTable* table,
                        const std::vector<oid_t>& column_ids,
                        const std::vector<Value>& values)
        : target_table_(table),
          column_ids_(column_ids),
          values_(values) {
        assert(column_ids.size() == values.size());
    }

    explicit UpdateNode(storage::DataTable* table,
                            const std::vector<oid_t>& column_ids,
                            const std::vector<Value>& values,
                            const ColumnExprs column_exprs)
            : target_table_(table),
              column_ids_(column_ids),
              values_(values),
              column_exprs_(column_exprs){
            assert(column_ids.size() == values.size());
    }

    explicit UpdateNode(storage::DataTable* table,
                            const ColumnExprs column_exprs)
            : target_table_(table),
              column_exprs_(column_exprs){
    }

    inline PlanNodeType GetPlanNodeType() const {
        return PLAN_NODE_TYPE_UPDATE;
    }

    storage::DataTable *GetTable() const {
        return target_table_;
    }

    std::string GetInfo() const {
        return target_table_->GetName();
    }

    const std::vector<oid_t>& GetUpdatedColumns() const {
        return column_ids_;
    }

    const std::vector<Value>& GetUpdatedColumnValues() const {
        return values_;
    }

    const ColumnExprs& GetUpdatedColumnExprs() const {
      return column_exprs_;
    }

private:

    /** @brief Target table. */
    storage::DataTable *target_table_;

    /** @brief Columns altered by update */
    const std::vector<oid_t> column_ids_;

    /** @brief Values of columns altered by update */
    const std::vector<Value> values_;

    /**
     * @brief Columns altered by an expression
     * TODO: The above two can actually be merged into this one (using Constant expressions).
     * But I keep them for now because I don't want to break existing tests.
     */
    const ColumnExprs column_exprs_;


};

} // namespace planner
} // namespace peloton
