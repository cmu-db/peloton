#include "backend/parser/statement_select.h"
#include "backend/parser/table_ref.h"

namespace nstore {
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
} // End nstore namespace
