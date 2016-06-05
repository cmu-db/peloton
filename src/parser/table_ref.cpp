#include "parser/statement_select.h"
#include "parser/table_ref.h"

namespace peloton {
namespace parser {

TableRef::~TableRef() {
  free(name);
	free(alias);
	free(schema);

	delete select;
  delete join;

	if(list) {
	  for(auto ref: (*list))
	    delete ref;
	  delete list;
	}
}

} // End parser namespace
} // End peloton namespace
