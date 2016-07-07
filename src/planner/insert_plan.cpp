
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_plan.cpp
//
// Identification: /peloton/src/planner/insert_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
 
#include "planner/insert_plan.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "parser/peloton/insert_parse.h"
#include "catalog/bootstrapper.h"

namespace peloton{
namespace planner{
InsertPlan::InsertPlan(storage::DataTable *table, oid_t bulk_insert_count)
    : target_table_(table), bulk_insert_count(bulk_insert_count) {
  catalog::Bootstrapper::bootstrap();
  catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME);
}

// This constructor takes in a project info
InsertPlan::InsertPlan(
    storage::DataTable *table,
    std::unique_ptr<const planner::ProjectInfo> &&project_info,
    oid_t bulk_insert_count)
    : target_table_(table),
      project_info_(std::move(project_info)),
      bulk_insert_count(bulk_insert_count) {
	  catalog::Bootstrapper::bootstrap();
	  catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME);
}

// This constructor takes in a tuple
InsertPlan::InsertPlan(storage::DataTable *table,
                    std::unique_ptr<storage::Tuple> &&tuple,
                    oid_t bulk_insert_count)
    : target_table_(table),
      tuple_(std::move(tuple)),
      bulk_insert_count(bulk_insert_count) {
	  catalog::Bootstrapper::bootstrap();
	  catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME);
}

// This constructor takes a parse tree
InsertPlan::InsertPlan(parser::InsertParse *parse_tree, oid_t bulk_insert_count) : bulk_insert_count(bulk_insert_count) {
  std::vector<std::string> columns = parse_tree->getColumns();
  std::vector<Value> values = parse_tree->getValues();
  catalog::Bootstrapper::bootstrap();
  catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME);
  target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(DEFAULT_DB_NAME, parse_tree->GetTableName());
  PL_ASSERT(target_table_);
  catalog::Schema* table_schema = target_table_->GetSchema();
  if(columns.size() == 0){
	    PL_ASSERT(values.size() == table_schema->GetColumnCount());
	    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table_schema, true));
	    int col_cntr = 0;
	    std::for_each(values.begin(), values.end(), [&](Value const& elem){
	    	LOG_INFO("Inside Planner. Value %d: %s", col_cntr, elem.GetInfo().c_str());
	    	switch (elem.GetValueType()) {
	    	  case VALUE_TYPE_VARCHAR:
	    	  case VALUE_TYPE_VARBINARY:
	    		  tuple->SetValue(col_cntr++, elem, GetPlanPool());
	    		  break;

	    	    default: {
	    	    	tuple->SetValue(col_cntr++, elem, nullptr);
	    	    }
	    	  }
	    });
	    tuple_ = std::move(tuple);
  }
  else{

  }
}

VarlenPool *InsertPlan::GetPlanPool() {
  // construct pool if needed
  if (pool_.get() == nullptr) pool_.reset(new VarlenPool(BACKEND_TYPE_MM));
  // return pool
  return pool_.get();
}

}
}


