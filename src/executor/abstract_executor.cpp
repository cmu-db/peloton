#include "abstract_executor.h"

namespace nstore {
namespace executor {

bool AbstractExecutor::Init() {
	//TODO Insert any code common to all executors here.

	Init();
}

storage::Tile *AbstractExecutor::GetNextTile() {

	//TODO Insert any code common to all executors here.

	return GetNextTile();
}

void AbstractExecutor::CleanUp() {

	//TODO Insert any code common to all executors here.

	CleanUp();
}

} // namespace executor
} // namespace nstore
