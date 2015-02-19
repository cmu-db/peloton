

#include "SQLParser.h"

#include <iostream>
#include <string>


namespace nstore {

namespace parser {

int ParseSQLString() {

	while (1) {
		std::string sql_stmt;
		std::cout << "Enter a SQL statement:\n";
		std::getline(std::cin, sql_stmt, ';');

		hsql::SQLStatementList* stmt_list = hsql::SQLParser::parseSQLString(sql_stmt);
		if (!stmt_list->isValid) {
			std::cout << "Failed!!!\n";
			continue;
		}

		std::cout << "Success!\n";

		for (unsigned int i = 0; i < stmt_list->numStatements(); i++) {
			hsql::SQLStatement *abs_stmt = stmt_list->getStatement(i);
			std::cout << "Statement type: " << abs_stmt->type() << std::endl;

			if (abs_stmt->type() == hsql::kStmtSelect) {
				hsql::SelectStatement *stmt = static_cast<hsql::SelectStatement *>(abs_stmt);
				hsql::TableRef *from_table = stmt->from_table;

				if (from_table->name != NULL) {
					std::cout << "Table name: " << from_table->name << std::endl;
				}

				if (from_table->join != NULL) {
					std::cout << "Joining tables "
							<< from_table->join->left->name << " and "
							<< from_table->join->right->name
							<< std::endl;
				}
			}
		}
	}
	return 0;
}

}

}
