#pragma once

#include "planner/abstract_plan_node.h"

namespace nstore {
namespace planner {
    
class ProjectionNode : public AbstractPlanNode {
  public:

    const std::vector<std::string>& OutputColumnNames() const {
        return output_column_names;
    }

    const std::vector<ValueType>& OutputColumnTypes() const {
        return output_column_types;
    }

    const std::vector<int32_t>& OutputColumnSizes() const {
        return output_column_sizes;
    }

  private:

    std::vector<std::string> output_column_names;

    std::vector<ValueType> output_column_types;

    std::vector<int32_t> output_column_sizes;
};

} // namespace planner
} // namespace nstore
