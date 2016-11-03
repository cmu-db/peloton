//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_test.cpp
//
// Identification: test/parser/parser_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "common/harness.h"
#include "common/macros.h"
#include "common/logger.h"
#include "parser/parser.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Parser Tests
//===--------------------------------------------------------------------===//

class ParserTest : public PelotonTest {};

TEST_F(ParserTest, BasicTest) {

  std::vector<std::string> queries;

  // SELECT statement
  queries.push_back("SELECT * FROM orders;");
  queries.push_back("SELECT a FROM orders;");
  queries.push_back(
      "SELECT a FROM foo WHERE a > 12 OR b > 3 AND NOT c LIMIT 10");
  queries.push_back(
      "SELECT * FROM foo where bar = 42 ORDER BY id DESC LIMIT 23;");

  queries.push_back(
      "SELECT col1 AS myname, col2, 'test' FROM \"table\", foo AS t WHERE age "
      "> 12 AND zipcode = 12345 GROUP BY col1;");
  queries.push_back(
      "SELECT * from \"table\" JOIN table2 ON a = b WHERE (b OR NOT a) AND a = "
      "12.5");
  queries.push_back(
      "(SELECT a FROM foo WHERE a > 12 OR b > 3 AND c NOT LIKE 's%' LIMIT "
      "10);");
  queries.push_back(
      "SELECT * FROM \"table\" LIMIT 10 OFFSET 10; SELECT * FROM second;");
  queries.push_back("SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY col1;");

  // JOIN
  queries.push_back(
      "SELECT t1.a, t1.b, t2.c FROM \"table\" AS t1 JOIN (SELECT * FROM foo "
      "JOIN bar ON foo.id = bar.id) t2 ON t1.a = t2.b WHERE (t1.b OR NOT t1.a) "
      "AND t2.c = 12.5");
  queries.push_back("SELECT * FROM t1 JOIN t2 ON c1 = c2;");
  queries.push_back("SELECT a, SUM(b) FROM t2 GROUP BY a HAVING SUM(b) > 100;");

  // CREATE statement
  queries.push_back(
      "CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, "
      "grade DOUBLE)");

  // Multiple statements
  queries.push_back(
      "CREATE TABLE students (name TEXT, student_number INTEGER); SELECT * "
      "FROM \"table\";");

  // INSERT
  queries.push_back("INSERT INTO test_table VALUES (1, 2, 'test');");
  queries.push_back("INSERT INTO test_table VALUES (1, 2, 'test'), (2, 3, 'test2');");
  queries.push_back("INSERT INTO test_table VALUES (1, 2, 'test'), (2, 3, 'test2'), (3, 4, 'test3');");
  queries.push_back(
      "INSERT INTO test_table (id, value, name) VALUES (1, 2, 'test');");
  queries.push_back(
        "INSERT INTO test_table (id, value, name) VALUES (1, 2, 'test'), (2, 3, 'test2');");
  queries.push_back(
          "INSERT INTO test_table (id, value, name) VALUES (1, 2, 'test'), (2, 3, 'test2'), (3, 4, 'test3');");
  queries.push_back("INSERT INTO test_table SELECT * FROM students;");

  // DELETE
  queries.push_back("DELETE FROM students WHERE grade > 3.0");
  queries.push_back("DELETE FROM students");
  queries.push_back("TRUNCATE students");

  // UPDATE
  queries.push_back(
      "UPDATE students SET grade = 1.3 WHERE name = 'Max Mustermann';");
  queries.push_back(
      "UPDATE students SET grade = 1.3, name='Felix Fürstenberg' WHERE name = "
      "'Max Mustermann';");
  queries.push_back("UPDATE students SET grade = 1.0;");

  // DROP
  queries.push_back("DROP TABLE students;");

  // PREPARE
  queries.push_back("PREPARE prep_inst: INSERT INTO test VALUES (?, ?, ?);");
  queries.push_back("EXECUTE prep_inst(1, 2, 3);");
  queries.push_back("EXECUTE prep;");

  queries.push_back(
      "COPY catalog_db.query_metric TO '/home/user/output.csv' DELIMITER ',';");

  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    parser::SQLStatementList* stmt_list =
        parser::Parser::ParseSQLString(query.c_str());
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(ParserTest, GrammarTest) {
  std::vector<std::string> valid_queries;

  valid_queries.push_back("SELECT * FROM test;");
  valid_queries.push_back(
      "SELECT name, address, age FROM customers WHERE age > 10 AND city = "
      "'Berlin';");
  valid_queries.push_back(
      "SELECT * FROM customers JOIN orders ON customers.id = "
      "orders.customer_id ORDER BY order_value;");

  for (auto query : valid_queries) {
    parser::SQLStatementList* result =
        parser::Parser::ParseSQLString(query.c_str());
    EXPECT_TRUE(result->is_valid);
    if (result->is_valid == false) {
      LOG_ERROR("Parsing failed: %s (%s)\n", query.c_str(), result->parser_msg);
    }
    delete result;
  }
}

#define EXPECT_NULL(pointer) EXPECT_TRUE(pointer == NULL);
#define EXPECT_NOTNULL(pointer) EXPECT_TRUE(pointer != NULL);

TEST_F(ParserTest, SelectParserTest) {
  std::string query =
      "SELECT customer_id, SUM(order_value) FROM order_db.customers JOIN "
      "orders ON customers.id = orders.customer_id GROUP BY customer_id ORDER "
      "BY SUM(order_value) DESC LIMIT 5;";

  parser::SQLStatementList* list =
      parser::Parser::ParseSQLString(query.c_str());
  EXPECT_TRUE(list->is_valid);
  if (list->is_valid == false) {
    LOG_ERROR("Parsing failed: %s (%s)\n", query.c_str(), list->parser_msg);
  }

  EXPECT_EQ(list->GetNumStatements(), 1);
  EXPECT_EQ(list->GetStatement(0)->GetType(), STATEMENT_TYPE_SELECT);

  parser::SelectStatement* stmt =
      (parser::SelectStatement*)list->GetStatement(0);

  EXPECT_NOTNULL(stmt->select_list);
  EXPECT_NOTNULL(stmt->from_table);
  EXPECT_NOTNULL(stmt->group_by);
  EXPECT_NOTNULL(stmt->order);
  EXPECT_NOTNULL(stmt->limit);

  EXPECT_NULL(stmt->where_clause);
  EXPECT_NULL(stmt->union_select);

  // Select List
  EXPECT_EQ(stmt->select_list->size(), 2);
  EXPECT_EQ(stmt->select_list->at(0)->GetExpressionType(),
            EXPRESSION_TYPE_COLUMN_REF);
  EXPECT_EQ(stmt->select_list->at(1)->GetExpressionType(),
            EXPRESSION_TYPE_FUNCTION_REF);

  // Join Table
  parser::JoinDefinition* join = stmt->from_table->join;
  EXPECT_EQ(stmt->from_table->type, TABLE_REFERENCE_TYPE_JOIN);
  EXPECT_NOTNULL(join);
  EXPECT_STREQ(join->left->GetTableName(), "customers");
  EXPECT_STREQ(join->right->GetTableName(), "orders");
  EXPECT_STREQ(join->left->GetDatabaseName(), "order_db");

  // Group By
  EXPECT_EQ(stmt->group_by->columns->size(), 1);

  // Order By
  EXPECT_EQ(stmt->order->type, parser::kOrderDesc);
  EXPECT_EQ(stmt->order->expr->GetExpressionType(),
            EXPRESSION_TYPE_FUNCTION_REF);

  // Limit
  EXPECT_EQ(stmt->limit->limit, 5);

  delete list;
}

TEST_F(ParserTest, TransactionTest) {
  std::vector<std::string> valid_queries;

  valid_queries.push_back("BEGIN TRANSACTION;");
  valid_queries.push_back("BEGIN;");
  valid_queries.push_back("COMMIT TRANSACTION;");
  valid_queries.push_back("ROLLBACK TRANSACTION;");

  for (auto query : valid_queries) {
    parser::SQLStatementList* result =
        parser::Parser::ParseSQLString(query.c_str());
    EXPECT_TRUE(result->is_valid);

    if (result->is_valid == false) {
      LOG_ERROR("Parsing failed: %s (%s)\n", query.c_str(), result->parser_msg);
    }
    LOG_INFO("%s", result->GetInfo().c_str());
    delete result;
  }

  parser::SQLStatementList* list =
      parser::Parser::ParseSQLString(valid_queries[0].c_str());
  parser::TransactionStatement* stmt =
      (parser::TransactionStatement*)list->GetStatement(0);
  EXPECT_EQ(list->GetStatement(0)->GetType(), STATEMENT_TYPE_TRANSACTION);
  EXPECT_EQ(stmt->type, parser::TransactionStatement::kBegin);
  delete list;

  list = parser::Parser::ParseSQLString(valid_queries[1].c_str());
  stmt = (parser::TransactionStatement*)list->GetStatement(0);
  EXPECT_EQ(stmt->type, parser::TransactionStatement::kBegin);
  delete list;

  list = parser::Parser::ParseSQLString(valid_queries[2].c_str());
  stmt = (parser::TransactionStatement*)list->GetStatement(0);
  EXPECT_EQ(stmt->type, parser::TransactionStatement::kCommit);
  delete list;

  list = parser::Parser::ParseSQLString(valid_queries[3].c_str());
  stmt = (parser::TransactionStatement*)list->GetStatement(0);
  EXPECT_EQ(stmt->type, parser::TransactionStatement::kRollback);
  delete list;
}

TEST_F(ParserTest, CreateTest) {
  std::vector<std::string> queries;

  queries.push_back(
      "CREATE TABLE ACCESS_INFO ("
      " s_id INTEGER"
      " )");

  queries.push_back(
      "CREATE TABLE ACCESS_INFO ("
      " s_id INTEGER PRIMARY KEY,"
      " ai_type TINYINT NOT NULL UNIQUE"
      " )");

  queries.push_back(
      "CREATE TABLE ACCESS_INFO ("
      " s_id INTEGER NOT NULL,"
      " ai_type TINYINT NOT NULL,"
      " PRIMARY KEY (s_id, ai_type),"
      " FOREIGN KEY (s_id) REFERENCES SUBSCRIBER (s_id)"
      " )");

  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    parser::SQLStatementList* result =
        parser::Parser::ParseSQLString(query.c_str());

    if (result->is_valid == false) {
      LOG_ERROR("Parsing failed: %s (%s)\n", query.c_str(), result->parser_msg);
    }
    EXPECT_EQ(result->is_valid, true);

    if (result) {
      LOG_INFO("%d : %s", ++ii, result->GetInfo().c_str());
      delete result;
    }
  }
}

TEST_F(ParserTest, TM1Test) {
  std::vector<std::string> queries;

  queries.push_back(
      "CREATE TABLE SUBSCRIBER ("
      " s_id INTEGER NOT NULL PRIMARY KEY,"
      " sub_nbr VARCHAR(15) NOT NULL UNIQUE,"
      " bit_1 TINYINT,"
      " bit_2 TINYINT,"
      " bit_3 TINYINT,"
      " byte2_1 SMALLINT,"
      " msc_location INTEGER,"
      " vlr_location INTEGER"
      ");");

  queries.push_back(
      "CREATE TABLE ACCESS_INFO ("
      "     s_id INTEGER NOT NULL,"
      "     ai_type TINYINT NOT NULL,"
      "     data1 SMALLINT,"
      " data2 SMALLINT,"
      "     data3 VARCHAR(3),"
      "     data4 VARCHAR(5),"
      "     PRIMARY KEY(s_id, ai_type),"
      "     FOREIGN KEY (s_id) REFERENCES SUBSCRIBER (s_id)"
      "  );");

  queries.push_back(
      "CREATE TABLE CALL_FORWARDING ("
      "s_id INTEGER NOT NULL,"
      "     sf_type TINYINT NOT NULL,"
      "     start_time TINYINT NOT NULL,"
      "     end_time TINYINT,"
      "     numberx VARCHAR(15),"
      "     PRIMARY KEY (s_id, sf_type, start_time),"
      "     FOREIGN KEY (s_id, sf_type) REFERENCES SPECIAL_FACILITY(s_id, "
      "sf_type)"
      "  );");

  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    parser::SQLStatementList* result =
        parser::Parser::ParseSQLString(query.c_str());

    if (result->is_valid == false) {
      LOG_ERROR("Parsing failed: %s (%s)\n", query.c_str(), result->parser_msg);
    }
    EXPECT_EQ(result->is_valid, true);

    if (result) {
      LOG_INFO("%d : %s", ++ii, result->GetInfo().c_str());
      delete result;
    }
  }
}

TEST_F(ParserTest, IndexTest) {

  std::vector<std::string> queries;

  queries.push_back(
      "CREATE INDEX i_security "
      " ON security (s_co_id, s_issue);");

  queries.push_back(
      "CREATE UNIQUE INDEX i_security "
      " ON security (s_co_id, s_issue);");

  queries.push_back("DROP INDEX i_security ON security;");
  queries.push_back("DROP DATABASE i_security;");

  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    parser::SQLStatementList* result =
        parser::Parser::ParseSQLString(query.c_str());

    if (result->is_valid == false) {
      LOG_ERROR("Parsing failed: %s (%s)\n", query.c_str(), result->parser_msg);
    }
    EXPECT_EQ(result->is_valid, true);

    if (result) {
      LOG_INFO("%d : %s", ++ii, result->GetInfo().c_str());
      delete result;
    }
  }
}

TEST_F(ParserTest, CopyTest) {

  std::vector<std::string> queries;

  queries.push_back(
      "COPY catalog_db.query_metric TO '/home/user/output.csv' DELIMITER ',';");

  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    parser::SQLStatementList* result =
        parser::Parser::ParseSQLString(query.c_str());

    if (result->is_valid == false) {
      LOG_ERROR("Parsing failed: %s (%s)\n", query.c_str(), result->parser_msg);
    }
    EXPECT_EQ(result->is_valid, true);

    parser::CopyStatement* copy_stmt =
        static_cast<parser::CopyStatement*>(result->GetStatement(0));

    EXPECT_STREQ(std::string(1, copy_stmt->GetDelimiter()).c_str(), ",");
    EXPECT_STREQ(copy_stmt->GetFilePath(), "/home/user/output.csv");

    if (result) {
      LOG_INFO("%d : %s", ++ii, result->GetInfo().c_str());
      delete result;
    }
  }
}

}  // End test namespace
}  // End peloton namespace
