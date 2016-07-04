//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// config.cpp
//
// Identification: src/common/config.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/config.h"

namespace peloton {

void PelotonConfiguration::PrintHelp() {
}

int PelotonConfiguration::GetPort() const{
  return port;
}

void PelotonConfiguration::SetPort(const int port_){
  port = port_;
}

int PelotonConfiguration::GetMaxConnections() const{
 return max_connections;
}

void PelotonConfiguration::SetMaxConnections(const int max_connections_){
  max_connections = max_connections_;
}

std::string PelotonConfiguration::GetSocketFamily() const{
  return socket_family;
}

void PelotonConfiguration::SetSocketFamily(const std::string& socket_family_){
  socket_family = socket_family_;
}

}  // End peloton namespace
