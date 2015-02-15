
#include "Table.h"
#include "SelectStatement.h"

namespace hsql {


TableRef::~TableRef() {
	delete name;
	delete alias;
	delete select;
	delete list;
}


} // namespace hsql