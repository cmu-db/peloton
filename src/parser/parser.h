/*-------------------------------------------------------------------------
 *
 * parser.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/parser/parser.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

namespace nstore {
namespace parser {

//===--------------------------------------------------------------------===//
// Parser
//===--------------------------------------------------------------------===//

class Parser {

 public:

  // Parse a given query
  static void ParseSQLString(std::string query);

};


} // End parser namespace
} // End nstore namespace
