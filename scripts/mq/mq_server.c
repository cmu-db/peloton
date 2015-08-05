/* mq_notify_thread.c

   Demonstrate message notification via threads on a POSIX message queue.

   Linux supports POSIX message queues since kernel 2.6.6.
*/
#include <pthread.h>
#include <mqueue.h>
#include <fcntl.h>              /* For definition of O_NONBLOCK */
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>

static void notifySetup(mqd_t *mqdp);

static void                     /* Thread notification function */
threadFunc(union sigval sv)
{
    ssize_t numRead;
    mqd_t *mqdp;
    void *buffer;
    struct mq_attr attr;

    printf("HANDLER START \n");

    mqdp = sv.sival_ptr;

    /* Determine mq_msgsize for message queue, and allocate an input buffer
       of that size */

    if (mq_getattr(*mqdp, &attr) == -1)
        perror("mq_getattr");

    buffer = malloc(attr.mq_msgsize);
    if (buffer == NULL)
        perror("malloc");

    /* Reregister for message notification */

    notifySetup(mqdp);

    while ((numRead = mq_receive(*mqdp, buffer, attr.mq_msgsize, NULL)) >= 0)
        printf("Read %ld bytes\n", (long) numRead);

    if (errno != EAGAIN)                        /* Unexpected error */
        perror("mq_receive");

    free(buffer);

    printf("HANDLER DONE \n");

    pthread_exit(NULL);
}

static void
notifySetup(mqd_t *mqdp)
{
    struct sigevent sev;

    printf("SETUP NOTIFY \n");

    sev.sigev_notify = SIGEV_THREAD;            /* Notify via thread */
    sev.sigev_notify_function = threadFunc;
    sev.sigev_notify_attributes = NULL;
            /* Could be pointer to pthread_attr_t structure */
    sev.sigev_value.sival_ptr = mqdp;           /* Argument to threadFunc() */

    if (mq_notify(*mqdp, &sev) == -1)
        perror("mq_notify");

    printf("FINISHED SETUP NOTIFY \n");

}

int
main(int argc, char *argv[])
{
    mqd_t mqd;

    if (argc != 2 || strcmp(argv[1], "--help") == 0) {
        fprintf(stderr, "%s mq-name\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    printf("OPEN \n");

    mqd = mq_open(argv[1], O_CREAT | O_RDONLY | O_NONBLOCK,  S_IRUSR | S_IWUSR, NULL);
    if (mqd == (mqd_t) -1)
        perror("mq_open");

    notifySetup(&mqd);

    printf("GOING TO PAUSE \n");
    pause();                    /* Wait for notifications via thread function */


    printf("SERVER DONE \n");
}
