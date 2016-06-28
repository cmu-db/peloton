
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// update_parse.cpp
//
// Identification: /peloton/src/parser/peloton/update_parse.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "parser/peloton/update_parse.h"

#include "common/types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

UpdateParse::UpdateParse(UpdateStmt* update_node) {
	if(update_node->withClause){
		// process with clause
	}
}

}  // namespace parser
}  // namespace peloton
