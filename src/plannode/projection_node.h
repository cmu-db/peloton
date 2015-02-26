#pragma once

namespace nstore {
namespace plannode {
    
class ProjectionNode : public AbstractPlanNode {
  public:
    const std::vector<std::string>& outputColumnNames() const {
        return output_column_names_;
    }

    const std::vector<ValueType>& outputColumnTypes() const {
        return output_column_types_;
    }

    const std::vector<int32_t>& outputColumnSizes const {
        return output_column_sizes_;
    }

  private:
    std::vecor<std::string> output_column_names_;
    std::vector<ValueType> output_column_types_;
    std::vector<int32_t> output_column_sizes_;
};

} // namespace plannode
} // namespace nstore
