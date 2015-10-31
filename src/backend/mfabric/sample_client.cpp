#include "nanomsg.h"
#include <iostream>
#include <cstring>
#include <cassert>

using namespace peloton;
using namespace mfabric;

int main(void)
{
	NanoMsg *m = new NanoMsg();
	int sock = m->CreateSocket(AF_SP, NN_PUSH);
	std::cout << "Socket Number: " << sock << std::endl;

	m->ConnectSocket(sock,"tcp://localhost:5656");

	char *msg = "This is a message";
	int sz_msg = strlen (msg);
        std::cout << "NODE1: SENDING " <<  msg << std::endl;
        int bytes = m->SendMessage(sock, msg, sz_msg, 0); 
        assert (bytes == sz_msg);

	m->CloseSocket(sock);
	m->ShutdownSocket(sock,0);
}

