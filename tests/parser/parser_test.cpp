/*-------------------------------------------------------------------------
 *
 * parser_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/parser/parser_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"

#include "harness.h"
#include "parser/parser.h"

#include <vector>

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Parser Tests
//===--------------------------------------------------------------------===//

TEST(ParserTests, BasicTest) {

  std::vector<std::string> queries;

  // SELECT statement
  queries.push_back("SELECT * FROM orders;");
  queries.push_back("SELECT a FROM foo WHERE a > 12 OR b > 3 AND NOT c LIMIT 10");
  queries.push_back("SELECT col1 AS myname, col2, 'test' FROM \"table\", foo AS t WHERE age > 12 AND zipcode = 12345 GROUP BY col1;");
  queries.push_back("SELECT * from \"table\" JOIN table2 ON a = b WHERE (b OR NOT a) AND a = 12.5");
  queries.push_back("(SELECT a FROM foo WHERE a > 12 OR b > 3 AND c NOT LIKE 's%' LIMIT 10);");
  queries.push_back("SELECT * FROM \"table\" LIMIT 10 OFFSET 10; SELECT * FROM second;");
  queries.push_back("SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY col1;");

  // JOIN
  queries.push_back("SELECT t1.a, t1.b, t2.c FROM \"table\" AS t1 JOIN (SELECT * FROM foo JOIN bar ON foo.id = bar.id) t2 ON t1.a = t2.b WHERE (t1.b OR NOT t1.a) AND t2.c = 12.5");
  queries.push_back("SELECT * FROM t1 JOIN t2 ON c1 = c2;");
  queries.push_back("SELECT a, SUM(b) FROM t2 GROUP BY a HAVING SUM(b) > 100;");

  // CREATE statement
  queries.push_back("CREATE TABLE \"table\" FROM TBL FILE 'students.tbl'");
  queries.push_back("CREATE TABLE IF NOT EXISTS \"table\" FROM TBL FILE 'students.tbl'");
  queries.push_back("CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE)");

  // Multiple statements
  queries.push_back("CREATE TABLE \"table\" FROM TBL FILE 'students.tbl'; SELECT * FROM \"table\";");

  // INSERT
  queries.push_back("INSERT INTO test_table VALUES (1, 2, 'test');");
  queries.push_back("INSERT INTO test_table (id, value, name) VALUES (1, 2, 'test');");
  queries.push_back("INSERT INTO test_table SELECT * FROM students;");

  // DELETE
  queries.push_back("DELETE FROM students WHERE grade > 3.0");
  queries.push_back("DELETE FROM students");
  queries.push_back("TRUNCATE students");

  // UPDATE
  queries.push_back("UPDATE students SET grade = 1.3 WHERE name = 'Max Mustermann';");
  queries.push_back("UPDATE students SET grade = 1.3, name='Felix Fürstenberg' WHERE name = 'Max Mustermann';");
  queries.push_back("UPDATE students SET grade = 1.0;");

  // DROP
  queries.push_back("DROP TABLE students;");

  // PREPARE
  queries.push_back("PREPARE prep_inst: INSERT INTO test VALUES (?, ?, ?);");
  queries.push_back("EXECUTE prep_inst(1, 2, 3);");
  queries.push_back("EXECUTE prep;");

  // Parsing
  for(auto query : queries) {
    parser::SQLStatementList* stmt_list = parser::Parser::ParseSQLString(query.c_str());
    std::cout << (*stmt_list);
    delete stmt_list;
  }

}

} // End test namespace
} // End nstore namespace

