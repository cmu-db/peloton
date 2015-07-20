/**
 * @brief Header for insert plan node.
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

class InsertNode : public AbstractPlanNode {
public:

    InsertNode() = delete;
    InsertNode(const InsertNode &) = delete;
    InsertNode& operator=(const InsertNode &) = delete;
    InsertNode(InsertNode &&) = delete;
    InsertNode& operator=(InsertNode &&) = delete;

    explicit InsertNode(storage::DataTable* table,
                        const expression::ProjExprVector &projs)
        : target_table_(table),
          projs_(projs) {
    }

    inline PlanNodeType GetPlanNodeType() const {
        return PLAN_NODE_TYPE_INSERT;
    }

    storage::DataTable *GetTable() const {
        return target_table_;
    }

    std::string GetInfo() const {
        return target_table_->GetName();
    }

    const expression::ProjExprVector& GetProjs() const {
        return projs_;
    }

private:
    /** @brief Target table. */
    storage::DataTable *target_table_;

    // proj list
    expression::ProjExprVector projs_;
};

} // namespace planner
} // namespace peloton

