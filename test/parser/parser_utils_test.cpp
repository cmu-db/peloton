//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_utils_test.cpp
//
// Identification: test/parser/parser_utils_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "common/harness.h"
#include "common/macros.h"
#include "common/logger.h"
#include "parser/postgresparser.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Parser Tests
//===--------------------------------------------------------------------===//

class ParserUtilTests : public PelotonTest {};

TEST_F(ParserUtilTests, BasicTest) {
  std::vector<std::string> queries;

  // SELECT statement
  queries.push_back("SELECT * FROM orders;");
  queries.push_back("SELECT MAX(*) FROM orders;");
  queries.push_back("SELECT MAX(price) FROM orders;");
  queries.push_back("SELECT a FROM orders;");
  queries.push_back("SELECT orders.a FROM orders;");
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
  queries.push_back(
      "SELECT player_name, year, "
      "CASE WHEN year = 'SR' THEN 'yes' "
      "ELSE NULL END AS is_a_senior "
      "FROM benn.college_football_players");

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
  queries.push_back(
      "INSERT INTO test_table VALUES (1, 2, 'test'), (2, 3, 'test2');");
  queries.push_back(
      "INSERT INTO test_table VALUES (1, 2, 'test'), (2, 3, 'test2'), (3, 4, "
      "'test3');");
  queries.push_back(
      "INSERT INTO test_table (id, value, name) VALUES (1, 2, 'test');");
  queries.push_back(
      "INSERT INTO test_table (id, value, name) VALUES (1, 2, 'test'), (2, 3, "
      "'test2');");
  queries.push_back(
      "INSERT INTO test_table (id, value, name) VALUES (1, 2, 'test'), (2, 3, "
      "'test2'), (3, 4, 'test3');");
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
  queries.push_back("DROP SCHEMA students;");
  queries.push_back("DROP TRIGGER tri ON students;");

  // PREPARE
  queries.push_back(
      "PREPARE prep_inst AS INSERT INTO test VALUES ($1, $2, $3);");

  queries.push_back(
      "COPY pg_catalog.query_metric TO '/home/user/output.csv' DELIMITER ',';");

  // ANALYZE
  queries.push_back("ANALYZE t ( col1, col2, col3 );");

  // EXECUTE
  queries.push_back("EXECUTE fooplan(1, 'Hunter Valley', 't', 200.00);");
  queries.push_back("EXECUTE prep_inst(1, 2, 3);");
  queries.push_back("EXECUTE prep;");

  // TRANSACTION
  queries.push_back("BEGIN TRANSACTION;");
  queries.push_back("COMMIT TRANSACTION;");
  queries.push_back("ROLLBACK TRANSACTION;");

  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser::PostgresParser::ParseSQLString(query));
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    for (auto &stmt : stmt_list->statements) {
      LOG_TRACE("%s\n", stmt->GetInfo().c_str());
      EXPECT_TRUE(stmt->GetInfo().size() > 0);
    }
  }
}

}  // namespace test
}  // namespace peloton
