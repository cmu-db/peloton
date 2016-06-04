//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// main.cpp
//
// Identification: src/main/main.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include <iostream>

#include "common/macros.h"
#include "brain/sample.h"
#include "wire/socket_base.h"
#include "wire/wire.h"

/*
 * Any Postgres server process begins execution here.
 */
int main(UNUSED_ATTRIBUTE int argc, UNUSED_ATTRIBUTE char *argv[]) {
  printf("foo\n");

  peloton::brain::Sample sample(2);
  std::cout << sample;

  return 0;

  if (argc != 2) {
    std::cout << "Usage: ./wire_server [port]" << std::endl;
    exit(EXIT_FAILURE);
  }
  peloton::wire::Server server(atoi(argv[1]), MAX_CONNECTIONS);
  peloton::wire::start_server(&server);
  peloton::wire::handle_connections<peloton::wire::PacketManager,
                                    peloton::wire::PktBuf>(&server);

  return 0;
}
