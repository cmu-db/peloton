/*-------------------------------------------------------------------------
 *
 * table_factory.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/table_factory.h
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/schema.h"

namespace nstore {
namespace storage {

class TempTable;

//===--------------------------------------------------------------------===//
// Table factory
//===--------------------------------------------------------------------===//

class TableFactory {
public:
	TableFactory();
	virtual ~TableFactory();


	/**
	 * Creates an empty temp table with given name and columns.
	 * Every TempTable must be instantiated via these factory methods.
	 * TempTable doesn't have constraints or indexes.
	 * And no logging.
	 */
	static Table* GetTempTable(Oid database_id,	const std::string &identifier,
			catalog::Schema* schema, const std::string* column_names, int size);

};

} // End storage namespace
} // End nstore namespace


