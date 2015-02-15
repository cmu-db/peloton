#include <iostream>
#include <string>

#include "third_party/parser/SQLParser.h"

using namespace hsql;

int main(void) {
    while (1) {
        std::string sql_stmt;
        std::cout << "Enter a SQL statement:\n";
        std::getline(std::cin, sql_stmt, ';');
        SQLStatementList* stmt_list = SQLParser::parseSQLString(sql_stmt);
        if (!stmt_list->isValid) {
            std::cout << "Failed!!!\n";
            continue;
        }
        std::cout << "Success!\n";
        for (unsigned int i = 0; i < stmt_list->numStatements(); i++) {
            SQLStatement *abs_stmt = stmt_list->getStatement(i);
            std::cout << "Statement type: " << abs_stmt->type() << std::endl;
            if (abs_stmt->type() == kStmtSelect) {
                SelectStatement *stmt = static_cast<SelectStatement *>(abs_stmt);
                TableRef *from_table = stmt->from_table;
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
