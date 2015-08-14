
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

#include "backend/common/types.h"

extern thread_local mqd_t MyBackendQueue;

namespace peloton {

std::string get_mq_name(oid_t id);

mqd_t create_mq(const std::string queue_name);

mqd_t open_mq(const std::string queue_name);

void send_message(mqd_t mqd, const std::string message);

void wait_for_message(mqd_t *mqdp);

}  // End peloton namespace
