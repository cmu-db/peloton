#pragma once

namespace nstore {
namespace parser {

#define TEST_PARSE_SQL_QUERY(query, output_var, num_statements) \
	SQLStatementList* output_var = SQLParser::parseSQLString(query); \
	ASSERT(output_var->isValid); \
	ASSERT_EQ(output_var->numStatements(), num_statements);


#define TEST_PARSE_SINGLE_SQL(query, stmt_type, stmt_class, output_var) \
	TEST_PARSE_SQL_QUERY(query, stmt_list, 1); \
    ASSERT_EQ(stmt_list->getStatement(0)->type(), stmt_type); \
    stmt_class* output_var = (stmt_class*) stmt_list->getStatement(0);


#define TEST_CAST_STMT(stmt_list, stmt_index, stmt_type, stmt_class, output_var) \
    ASSERT_EQ(stmt_list->getStatement(stmt_index)->type(), stmt_type); \
    stmt_class* output_var = (stmt_class*) stmt_list->getStatement(stmt_index);

} // End parser namespace
} // End nstore namespace
