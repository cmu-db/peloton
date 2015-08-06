

#include <signal.h>

#include "backend/common/message_queue.h"
#include "backend/common/logger.h"

namespace peloton {

std::string get_message_queue_name(oid_t id) {
  return std::string("/backend_" + std::to_string(id));
}


mqd_t create_message_queue(const std::string queue_name) {

  mqd_t mqd = mq_open(queue_name.c_str(),
                      O_CREAT | O_RDONLY | O_NONBLOCK,
                      S_IRUSR | S_IWUSR,
                      NULL);
  if (mqd == (mqd_t) -1) {
    LOG_ERROR("mq_open during create : %s pid : %d \n",
              queue_name.c_str(), getpid());
    return mqd;
  }

  printf("CREATED QUEUE :: %s getpid : %d \n",
         queue_name.c_str(), getpid());

  return mqd;
}

mqd_t open_message_queue(const std::string queue_name) {
  int flags;

  flags = O_WRONLY | O_NONBLOCK;

  mqd_t mqd = mq_open(queue_name.c_str(), flags);
  if (mqd == (mqd_t) -1) {
    perror("mq_open during open");
  }

  return mqd;
}

void send_message(mqd_t mqd, const std::string message) {

  int prio = 0;

  printf("TRYING TO SEND MESSAGE :: %s \n", message.c_str());

  if (mq_send(mqd, message.c_str(), message.size(), prio) == -1) {
    perror("mq_send");
  }

  printf("SENT MESSAGE \n");
}

void receive_message(union sigval sv) {
  ssize_t numRead;
  mqd_t *mqdp;
  void *buffer;
  struct mq_attr attr;

  printf("HANDLER START :: pid : %d \n", getpid());

  mqdp = (mqd_t *) sv.sival_ptr;

  // Determine mq_msgsize for message queue, and allocate space
  if (mq_getattr(*mqdp, &attr) == -1)
    perror("mq_getattr");

  buffer = malloc(attr.mq_msgsize);
  if (buffer == NULL)
    perror("malloc");

  while ((numRead = mq_receive(*mqdp, (char *) buffer, attr.mq_msgsize, NULL)) >= 0)
    printf("Read %ld bytes\n", (long) numRead);

  if (errno != EAGAIN)                        /* Unexpected error */
    perror("mq_receive");

  free(buffer);
  printf("HANDLER DONE \n");

  kill(getpid(), SIGCONT);

  pthread_exit(NULL);
}


void notify_message(mqd_t *mqdp)
{
    struct sigevent sev;

    printf("SETUP NOTIFY \n");

    sev.sigev_notify = SIGEV_THREAD;            /* Notify via thread */
    sev.sigev_notify_function = receive_message;
    sev.sigev_notify_attributes = NULL;
            /* Could be pointer to pthread_attr_t structure */
    sev.sigev_value.sival_ptr = mqdp;           /* Argument to threadFunc() */

    if (mq_notify(*mqdp, &sev) == -1)
        perror("mq_notify");

    printf("FINISHED SETUP NOTIFY \n");

}

void wait_for_message(mqd_t *mqdp) {
  notify_message(mqdp);

  printf("GOING TO PAUSE :: pid : %d \n", getpid());

  pause();

  printf("FINISH PAUSE \n");
}

}  // End peloton namespace
