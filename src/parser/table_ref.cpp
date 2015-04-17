#include "statement_select.h"
#include "table_ref.h"

namespace nstore {
namespace parser {

TableRef::~TableRef() {
  delete name;
	delete alias;
	delete select;
	delete list;
}

} // End parser namespace
} // End nstore namespace
