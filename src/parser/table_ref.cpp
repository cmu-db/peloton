#include "statement_select.h"
#include "table_ref.h"

namespace nstore {
namespace parser {

TableRef::~TableRef() {
  free(name);
	free(alias);

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
