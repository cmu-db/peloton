
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// delete_parse.cpp
//
// Identification: /peloton/src/parser/peloton/delete_parse.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
 
#include "parser/peloton/update_parse.h"

#include "common/types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

DeleteParse::DeleteParse(DeleteStmt* delete_node) {

	//Get Table name
	entity_name = delete_node->relation->relname;

	// if query has a With Clause then it will be processed seperatly

	if(delete_node->withClause){
		// process with clause
	}
}

}  // namespace parser
}  // namespace peloton



