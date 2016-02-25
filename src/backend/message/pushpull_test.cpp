#include "nanomsg.h"
#include <iostream>
#include <cassert>
#include <cstring>
#include <unistd.h>
#include <stdlib.h>

using namespace peloton;
using namespace mfabric;

int server(void)
{
	std::cout << "Server PID: " << getpid() << std::endl;
	NanoMsg *m = new NanoMsg();
	int sock = m->CreateSocket(AF_SP, NN_PAIR);
	std::cout << "Socket Number: " << sock << std::endl;

	int eid = m->BindSocket(sock,"tcp://*:5656");

	char *buf = NULL;
	int bytes = m->ReceiveMessage(sock, &buf, NN_MSG, 0); 
	assert (bytes >= 0); 
	std::cout << "NODE0: RECEIVED " << buf << std::endl;
//	m->ShutdownSocket(sock,eid);

	sleep(5);
	m->CloseSocket(sock);
	delete(m);
}

int client(void)
{
	std::cout << "Client PID: " << getpid() << std::endl;
	NanoMsg *m = new NanoMsg();
	int sock = m->CreateSocket(AF_SP, NN_PAIR);
	std::cout << "Socket Number: " << sock << std::endl;

	int eid = m->ConnectSocket(sock,"tcp://localhost:5656");

	char *msg = "This is a message";
	int sz_msg = strlen (msg) + 1;
	std::cout << "NODE1: SENDING " <<  msg << std::endl;
	int bytes = m->SendMessage(sock, msg, sz_msg, 0); 
	assert (bytes == sz_msg);
//	m->ShutdownSocket(sock,eid);

	m->CloseSocket(sock);
	delete(m);
}

int main(void)
{
	NanoMsg *s = new NanoMsg();
	int ssock = s->CreateSocket(AF_SP, NN_PAIR);
	int seid = s->BindSocket(ssock,"tcp://*:5656");

	NanoMsg *c = new NanoMsg();
	int csock = c->CreateSocket(AF_SP, NN_PAIR);
	int ceid = c->ConnectSocket(csock,"tcp://localhost:5656");

	char *msg = "This is a message";
	int sz_msg = strlen (msg) + 1;
	int bytes = c->SendMessage(csock, msg, sz_msg, 0); 

	char *buf = NULL;
	int sbytes = s->ReceiveMessage(ssock, &buf, NN_MSG, 0); 
	std::cout << "Server got: " << buf << std::endl;

	c->CloseSocket(csock);
	s->CloseSocket(ssock);
	delete(c);
	delete(s);
}
