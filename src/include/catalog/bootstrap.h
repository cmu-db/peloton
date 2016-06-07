//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bootstrap.h
//
// Identification: src/include/catalog/bootstrap.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/data_table.h"
#include <string>


namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Bootstrap
//===--------------------------------------------------------------------===//

class Bootstrap{

	// Create Table
	storage::DataTable CreateTable(oid_t table_oid, std::string table_name);



};

}
}
