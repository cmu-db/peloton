

#include "backend/common/message_queue.h"

namespace peloton {

mqd_t create_message_queue(const std::string queue_name) {
  int flags;

  flags = O_CREAT | O_RDONLY | O_NONBLOCK;

  mqd_t mqd = mq_open(queue_name.c_str(), flags, S_IRUSR | S_IWUSR, NULL);
  if (mqd == (mqd_t) -1) {
    perror("mq_open during create");
  }

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

  if (mq_send(mqd, message.c_str(), message.size(), prio) == -1) {
    perror("mq_send");
  }

}

void receive_message(union sigval sv) {
  ssize_t numRead;
  mqd_t *mqdp;
  void *buffer;
  struct mq_attr attr;

  printf("HANDLER START \n");

  mqdp = (mqd_t *) sv.sival_ptr;

  // Determine mq_msgsize for message queue, and allocate space
  if (mq_getattr(*mqdp, &attr) == -1)
    perror("mq_getattr");

  buffer = malloc(attr.mq_msgsize);
  if (buffer == NULL)
    perror("malloc");

  // Reregister for message notification
  notify_message(mqdp);

  while ((numRead = mq_receive(*mqdp, (char *) buffer, attr.mq_msgsize, NULL)) >= 0)
    printf("Read %ld bytes\n", (long) numRead);

  if (errno != EAGAIN)                        /* Unexpected error */
    perror("mq_receive");

  free(buffer);
  printf("HANDLER DONE \n");

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

  printf("GOING TO PAUSE \n");

  pause();                    /* Wait for notifications via thread function */
}

}  // End peloton namespace
