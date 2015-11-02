//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nanomsg.cpp
//
// Identification: src/backend/mfabric/nanomsg.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "nanomsg.h"
#include <iostream>

namespace peloton {
namespace mfabric {

  /**
   * 
   */
  int NanoMsg::CreateSocket(int domain, int protocol)
  {
  	std::cout << "NanoMsg Create Called" << std::endl;
	return nn_socket (domain, protocol);
  }

  /**
   * TODO: Add port in argument list and convert it to string
   */
  int NanoMsg::BindSocket(int socket, const char *address)
  {
	std::cout << "NanoMsg Bind Called"<< std::endl;
	return nn_bind(socket,address);
  }

  /**
   * 
   */
  int NanoMsg::SendMessage(int socket, const void *buffer, size_t length, int flags)
  {
	std::cout << "NanoMsg SendMsg Called"<< std::endl;
	return nn_send (socket, buffer, length, flags);
  }

  /**
   * 
   */
  int NanoMsg::SetSocket(int socket, int level, int option, const void *opt_val, size_t opt_val_len)
  {
	std::cout << "NanoMsg SetSocket Called"<< std::endl;
	return nn_setsockopt(socket,level,option,opt_val,opt_val_len);
  }

  /**
   * 
   */
  int NanoMsg::ConnectSocket(int socket, const char *address)
  {
	std::cout << "NanoMsg ConnectSocket Called"<< std::endl;
	return nn_connect (socket, address);
  }

  /**
   * 
   */
  int NanoMsg::ReceiveMessage(int socket, void *buffer, size_t length, int flags)
  {
	std::cout << "NanoMsg ReceiveMessage Called"<< std::endl;
	return nn_recv(socket, buffer, length, flags);
  }

  /**
   * 
   */
  int NanoMsg::CloseSocket(int socket)
  {
	std::cout << "NanoMsg CloseSocket Called"<< std::endl;
	return nn_close(socket);
  }

  /**
   * 
   */
  int NanoMsg::ShutdownSocket(int socket,int how)
  {
	std::cout << "NanoMsg ShutdownSocket Called"<< std::endl;
	return nn_shutdown(socket,how);
  }

}
}
