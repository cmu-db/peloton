#include "nanomsg.h"
#include <iostream>
#include <cassert>

using namespace peloton;
using namespace mfabric;

int main(void)
{
	NanoMsg *m = new NanoMsg();
	int sock = m->CreateSocket(AF_SP, NN_PULL);
	std::cout << "Socket Number: " << sock << std::endl;

	m->BindSocket(sock,"tcp://*:5656");

        char *buf = NULL;
        int bytes = m->ReceiveMessage(sock, &buf, NN_MSG, 0); 
        assert (bytes >= 0); 
        std::cout << "NODE0: RECEIVED " << buf << std::endl;

	m->CloseSocket(sock);
	m->ShutdownSocket(sock,0);
}

