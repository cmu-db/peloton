
#include <stdio.h>
#include <string>
#include <chrono>
#include <fstream>
#include <sstream>
#include <vector>

#include "sql_parser.h"

namespace nstore {
namespace parser {

std::vector<std::string> readlines(std::string path) {
    std::ifstream infile(path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(infile, line)) {
        std::istringstream iss(line);

        // Skip comments
        if (line[0] != '#') {
            lines.push_back(line);
        }
    }
    return lines;
}

#define STREQ(a, b) (std::string(a).compare(std::string(b)) == 0)

int main(int argc, char *argv[]) {
    if (argc <= 1) {
        fprintf(stderr, "Usage: grammar_test [--false] [-f path] query, ...\n");
        return -1;
    }

    bool expect_false = false;
    bool use_file = false;
    std::string file_path = "";
    
    // Parse command line arguments
    int i = 1;
    for (; i < argc; ++i) {
        if (STREQ(argv[i], "--false")) expect_false = true;
        else if (STREQ(argv[i], "-f")) {
            use_file = true;
            file_path = argv[++i];
        } else {
            break;
        }
    }


    // Read list of queries for this rest
    std::vector<std::string> queries;
    if (use_file) {
        queries = readlines(file_path);
    } else {
        for (; i < argc; ++i) queries.push_back(argv[i]);
    }


    // Execute queries
    int num_failed = 0;
    for (std::string sql : queries) {
        // Measuring the parsing time
        std::chrono::time_point<std::chrono::system_clock> start, end;
        start = std::chrono::system_clock::now();

        // Parsing
        SQLStatementList* stmt_list = SQLParser::parseSQLString(sql.c_str());

        end = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed_seconds = end-start;
        double us = elapsed_seconds.count() * 1000 * 1000;

        if (expect_false == stmt_list->isValid) {
            printf("\033[0;31m{  failed}\033[0m\n");
            printf("\t\033[0;31m%s (L%d:%d)\n\033[0m", stmt_list->parser_msg, stmt_list->error_line, stmt_list->error_col);
            printf("\t%s\n", sql.c_str());
            num_failed++;
        } else {
            // TODO: indicate whether expect_false was set
            printf("\033[0;32m{      ok} (%.1fus)\033[0m %s\n", us, sql.c_str());
        }
    }

    if (num_failed == 0) {
        printf("\033[0;32m{      ok} \033[0mAll %lu grammar tests completed successfully!\n", queries.size());
    } else {
        fprintf(stderr, "\033[0;31m{  failed} \033[0mSome grammar tests failed! %d out of %lu tests failed!\n", num_failed, queries.size());
    }


	return 0;
}

} // End parser namespace
} // End nstore namespace
