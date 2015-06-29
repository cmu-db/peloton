/**
 * @brief Header for abstract plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <cstdint>
#include <vector>
#include <map>
#include <vector>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_plan_node.h"

namespace peloton {

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Join Plan Node
//===--------------------------------------------------------------------===//

class AbstractJoinPlanNode : public AbstractPlanNode {
public:
    AbstractJoinPlanNode(const AbstractJoinPlanNode &) = delete;
    AbstractJoinPlanNode& operator=(const AbstractJoinPlanNode &) = delete;
    AbstractJoinPlanNode(AbstractJoinPlanNode &&) = delete;
    AbstractJoinPlanNode& operator=(AbstractJoinPlanNode &&) = delete;

    AbstractJoinPlanNode(JoinType joinType, expression::AbstractExpression *predicate) :
        AbstractPlanNode(), 
        joinType_(joinType), predicate_(predicate) {
            // Fuck off!
    }

    //===--------------------------------------------------------------------===//
    // Accessors
    //===--------------------------------------------------------------------===//

    JoinType GetJoinType() const {
        return joinType_;
    }

    const expression::AbstractExpression *GetPredicate() const {
        return predicate_.get();
    }
    

private:

    /** @brief The type of join that we're going to perform */
    JoinType joinType_;

    /** @brief Join predicate. */
    const std::unique_ptr<expression::AbstractExpression> predicate_;

};

} // namespace planner
} // namespace peloton
