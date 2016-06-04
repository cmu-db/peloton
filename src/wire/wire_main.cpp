#include <iostream>
#include "socket_base.h"
#include "wire.h"

int main(int argc, char* argv[]) {
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
