/**
 * @brief Header for delete plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "planner/abstract_plan_node.h"
#include "common/types.h"
#include "storage/data_table.h"

namespace nstore {
namespace planner {

class UpdateNode : public AbstractPlanNode {
public:
    UpdateNode() = delete;
    UpdateNode(const UpdateNode &) = delete;
    UpdateNode& operator=(const UpdateNode &) = delete;
    UpdateNode(UpdateNode &&) = delete;
    UpdateNode& operator=(UpdateNode &&) = delete;

    explicit UpdateNode(storage::DataTable* table,
                        const std::vector<oid_t>& column_ids,
                        const std::vector<Value>& values)
        : target_table_(table),
          column_ids(column_ids),
          values(values) {
        assert(column_ids.size() == values.size());
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
        return column_ids;
    }

    const std::vector<Value>& GetUpdatedColumnValues() const {
        return values;
    }

private:

    /** @brief Target table. */
    storage::DataTable *target_table_;

    /** @brief Columns altered by update */
    const std::vector<oid_t>& column_ids;

    /** @brief Values of columns altered by update */
    const std::vector<Value>& values;
};

} // namespace planner
} // namespace nstore
