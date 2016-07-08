//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// config.h
//
// Identification: src/include/common/config.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>

namespace peloton {

class PelotonConfiguration {

 public:

  static void PrintHelp();

  int GetPort() const;

  void SetPort(const int port);

  int GetMaxConnections() const;

  void SetMaxConnections(const int max_connections);

  std::string GetSocketFamily() const;

  void SetSocketFamily(const std::string& socket_family);

 protected:

  // Peloton port
  int port = 5432;

  // Maximum number of connections
  int max_connections = 64;

  // Socket family (AF_UNIX, AF_INET)
  std::string socket_family = "AF_INET";

};

}  // End peloton namespace
