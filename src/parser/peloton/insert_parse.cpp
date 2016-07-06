//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_parse.cpp
//
// Identification: /peloton/src/parser/peloton/insert_parse.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "parser/peloton/insert_parse.h"

#include "common/logger.h"
#include "catalog/column.h"
#include "common/value.h"
#include "common/value_factory.h"

namespace peloton {
namespace parser {
InsertParse::InsertParse(InsertStmt* insert_node) {
	//Clear Vectors
	columns.clear();
	values.clear();

	//Get Table Name
	entity_name = insert_node->relation->relname;

	//Check if there are columns specified in query
	if (insert_node->cols != NIL) {
		LOG_INFO("--------- I am in the Columns loop--------");
		List* cols = insert_node->cols;
		ListCell* item;
		foreach(item, cols)
		{
			::Value *value = (::Value *) lfirst(item);
			LOG_INFO("Value of Integer -------> %s", strVal(value));
			columns.push_back(std::string(strVal(value)));
		}

	}

	SelectStmt* select = insert_node->selectStmt;

	List* value_list = select->valuesLists;
	ListCell* value_item;
	Value tuple_value;

	foreach(value_item, value_list)
	{
		LOG_INFO("---------------I am in the values loooop--------------");
		List* value_item_list = (List*) lfirst(value_item);
		ListCell* sub_value_item;

		foreach(sub_value_item, value_item_list){
		Node *tm = (Node *) lfirst(sub_value_item);

		if (IsA(tm, A_Const)) {
			A_Const *ac = (A_Const *) tm;

			if (IsA(&ac->val, Integer)) {

				LOG_INFO("Value of Integer -------> %d", (int) ac->val.val.ival);
				tuple_value = ValueFactory::GetIntegerValue((int) ac->val.val.ival);

			} else if (IsA(&ac->val, Float)) {
				//::Value* val = (::Value*) lfirst(sub_value_item);
				LOG_INFO("Value of Float -------> %s", ac->val.val.str);
				tuple_value = ValueFactory::GetDoubleValue(atof(ac->val.val.str));
			} else if(IsA(&ac->val, String)){
				//::Value* val = (::Value*) lfirst(sub_value_item);
				LOG_INFO("Value of Integer -------> %s", ac->val.val);
				tuple_value = ValueFactory::GetStringValue(std::string(ac->val.val.str));
			}

		}
		values.push_back(tuple_value);
	}
		LOG_INFO("Size of Values vector ------> %d" , values.size());
		LOG_INFO("Size of Columns vector ------> %d" , columns.size());
	}
}
}
}
