

#include <signal.h>

#include "backend/common/message_queue.h"
#include "backend/common/logger.h"

#define NOTIFY_SIG SIGUSR1
#define MAX_MESSAGE_SIZE 8192

namespace peloton {

void notify_message(mqd_t *mqdp);

std::string get_mq_name(oid_t id) {
  return std::string("/backend_" + std::to_string(id));
}

mqd_t create_mq(const std::string queue_name) {

  mqd_t mqd = mq_open(queue_name.c_str(),
                      O_CREAT | O_RDONLY | O_NONBLOCK,
                      S_IRUSR | S_IWUSR,
                      NULL);
  if (mqd == (mqd_t) -1) {
    LOG_ERROR("mq_open during create : %s pid : %d \n",
              queue_name.c_str(), getpid());
    return mqd;
  }
  LOG_TRACE("CREATED QUEUE :: %s getpid : %d \n",
         queue_name.c_str(), getpid());

  return mqd;
}

mqd_t open_mq(const std::string queue_name) {
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
  LOG_TRACE("TRYING TO SEND MESSAGE :: %s \n", message.c_str());

  if (mq_send(mqd, message.c_str(), message.size(), prio) == -1) {
    perror("mq_send");
  }

  LOG_TRACE("SENT MESSAGE \n");
}

static void
handler(int sig)
{
  (void)sig;
  // Just interrupt sigsuspend()
}

void receive_message(mqd_t *mqdp) {
  ssize_t chars_read;
  char buffer[MAX_MESSAGE_SIZE];
  struct mq_attr attr;
  LOG_TRACE("HANDLER :: pid : %d \n", getpid());

  // Determine mq_msgsize for message queue, and allocate space
  if (mq_getattr(*mqdp, &attr) == -1)
    perror("mq_getattr");

  while ((chars_read = mq_receive(*mqdp, buffer, MAX_MESSAGE_SIZE, NULL)) >= 0)
    LOG_TRACE("Read %ld bytes\n", (long) chars_read);

  if (errno != EAGAIN)  // Unexpected error
    perror("mq_receive");
}


void notify_message(mqd_t *mqdp)
{
  struct sigevent sev;
  sigset_t blockMask;
  struct sigaction sa;

  LOG_TRACE("SETUP NOTIFY \n");

  // Block the notification signal and establish a handler for it
  sigemptyset(&blockMask);
  sigaddset(&blockMask, NOTIFY_SIG);
  if (sigprocmask(SIG_BLOCK, &blockMask, NULL) == -1)
    perror("sigprocmask");

  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sa.sa_handler = handler;
  if (sigaction(NOTIFY_SIG, &sa, NULL) == -1)
    perror("sigaction");

  // Register for message notification via a signal
  sev.sigev_notify = SIGEV_SIGNAL;
  sev.sigev_signo = NOTIFY_SIG;

  if (mq_notify(*mqdp, &sev) == -1)
    perror("mq_notify");

}

void wait_for_message(mqd_t *mqdp) {
  sigset_t emptyMask;

  notify_message(mqdp);
  LOG_TRACE("SUSPENDING :: pid : %d \n", getpid());

  sigemptyset(&emptyMask);
  sigsuspend(&emptyMask); // Wait for notification signal

  LOG_TRACE("WOKE UP :: pid : %d \n", getpid());

  receive_message(mqdp);
}

}  // End peloton namespace
