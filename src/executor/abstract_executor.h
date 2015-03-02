
#pragma once

#include "../planner/abstract_plan_node.h"
#include "storage/tile.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// Abstract Executor
//===--------------------------------------------------------------------===//

class AbstractExecutor {
public:

	virtual bool Init() = 0;

	virtual storage::Tile *GetNextTile() = 0;

	virtual void CleanUp() = 0;

	virtual ~AbstractExecutor(){
		delete plan_node;
	}

protected:

	AbstractExecutor(planner::AbstractPlanNode *abstract_node) {
		plan_node = abstract_node;
	}

private:

	planner::AbstractPlanNode *plan_node;
};

} // namespace executor
} // namespace nstore
