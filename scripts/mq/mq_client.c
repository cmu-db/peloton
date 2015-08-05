/* pmsg_send.c

   Usage as shown in usageError().

   Send a message (specified as a command line argument) to a
   POSIX message queue.

   See also pmsg_receive.c.

   Linux supports POSIX message queues since kernel 2.6.6.
*/
#include <mqueue.h>
#include <fcntl.h>              /* For definition of O_NONBLOCK */
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>

static void
usageError(const char *progName)
{
    fprintf(stderr, "Usage: %s [-n] mq-name msg [prio]\n", progName);
    fprintf(stderr, "    -n           Use O_NONBLOCK flag\n");
    exit(EXIT_FAILURE);
}

int
main(int argc, char *argv[])
{
    int flags, opt;
    mqd_t mqd;
    unsigned int prio;

    flags = O_WRONLY;
    while ((opt = getopt(argc, argv, "n")) != -1) {
        switch (opt) {
        case 'n':   flags |= O_NONBLOCK;        break;
        default:    usageError(argv[0]);
        }
    }

    if (optind + 1 >= argc)
        usageError(argv[0]);

    printf("OPEN \n");

    mqd = mq_open(argv[optind], flags);
    if (mqd == (mqd_t) -1)
        perror("mq_open");

    prio = (argc > optind + 2) ? atoi(argv[optind + 2]) : 0;

    printf("SEND \n");

    if (mq_send(mqd, argv[optind + 1], strlen(argv[optind + 1]), prio) == -1)
      perror("mq_send");

    printf("CLIENT DONE \n");


    exit(EXIT_SUCCESS);
}
