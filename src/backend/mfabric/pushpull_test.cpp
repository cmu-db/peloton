#include "nanomsg.h"
#include <iostream>
#include <cassert>
#include <cstring>
#include <unistd.h>

using namespace peloton;
using namespace mfabric;

int server(void)
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

int client(void)
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

int main(void)
{
	pid_t fork_id = fork();
	if (fork_id == 0)
	{
		server();
	}
	else
	{
		sleep(2);
		client();
	}
}
