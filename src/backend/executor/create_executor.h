/*-------------------------------------------------------------------------
 *
 * create_executor.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/create_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/catalog.h"
#include "parser/parser.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// Create Statements
//===--------------------------------------------------------------------===//

class CreateExecutor {
public:

    CreateExecutor(const CreateExecutor &) = delete;
    CreateExecutor& operator=(const CreateExecutor &) = delete;
    CreateExecutor(CreateExecutor &&) = delete;
    CreateExecutor& operator=(CreateExecutor &&) = delete;

    static bool Execute(parser::SQLStatement *query);

protected:
    
    static bool CreateDatabase(parser::CreateStatement* stmt);
    static bool CreateTable(catalog::Database* db, parser::CreateStatement* stmt);
    static bool CreateIndex(catalog::Database* db, parser::CreateStatement* stmt);
    static bool CreateConstraint(catalog::Database* db, parser::CreateStatement* stmt);
    
};

} // namespace executor
} // namespace nstore

