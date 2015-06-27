/**
 * @brief Header for insert plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/planner/abstract_plan_node.h"
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
                        const std::vector<storage::Tuple *>& tuples)
        : target_table_(table),
          tuples(tuples) {
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

    const std::vector<storage::Tuple *>& GetTuples() const {
        return tuples;
    }

private:
    /** @brief Target table. */
    storage::DataTable *target_table_;

    // tuples to be inserted
    std::vector<storage::Tuple *> tuples;
};

} // namespace planner
} // namespace peloton
