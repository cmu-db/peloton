
#pragma once

#include <mqueue.h>
#include <fcntl.h>              /* For definition of O_NONBLOCK */
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>

#include <string>

namespace peloton {

mqd_t create_message_queue(const std::string queue_name);

mqd_t open_message_queue(const std::string queue_name);

void send_message(mqd_t mqd);

void receive_message(union sigval sv);

void notify_message(mqd_t *mqdp);

void wait_for_message(mqd_t *mqdp);

}  // End peloton namespace
