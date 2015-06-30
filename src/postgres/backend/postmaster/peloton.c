/*-------------------------------------------------------------------------
 *
 * peloton.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/peloton.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include "bridge/bridge.h"
#include "libpq/ip.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
#include "utils/memutils.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"

#include "postmaster/peloton.h"

#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>

/* ----------
 * Local data
 * ----------
 */
NON_EXEC_STATIC pgsocket pelotonSock = PGINVALID_SOCKET;

static struct sockaddr_storage pelotonAddr;

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile bool need_exit = false;
static volatile sig_atomic_t got_SIGHUP = false;

/* Flags to tell if we are in an peloton process */
static bool am_peloton = false;

/* ----------
 * Local function forward declarations
 * ----------
 */
NON_EXEC_STATIC void PelotonMain(int argc, char *argv[]) pg_attribute_noreturn();
static void peloton_MainLoop(void);
static void peloton_sighup_handler(SIGNAL_ARGS);
static void peloton_sigusr2_handler(SIGNAL_ARGS);
static void peloton_sigterm_handler(SIGNAL_ARGS);

static void peloton_setheader(Peloton_MsgHdr *hdr, PelotonMsgType mtype);
static void peloton_send(void *msg, int len);

static void peloton_recv_plan(Peloton_MsgPlan *msg, int len);


bool
IsPelotonProcess(void)
{
  return am_peloton;
}

/**
 * @brief Initialize peloton
 */
int peloton_start(void){
  pid_t   pelotonPid;

  switch ((pelotonPid = fork_process()))
  {
    case -1:
      ereport(LOG,
          (errmsg("could not fork peloton process: %m")));
      return -1;
      break;

    case 0:
      /* in postmaster child ... */
      InitPostmasterChild();

      /* Close the postmaster's sockets */
      ClosePostmasterPorts(false);

      /* Do some stuff */
      PelotonMain(0, NULL);
      return 0;
      break;

    default:
      /* Do nothing */
      return (int) pelotonPid;
      break;
  }

  /* shouldn't get here */
  return 0;
}

/*
 * PelotonMain
 *
 *  The argc/argv parameters are valid only in EXEC_BACKEND case.  However,
 *  since we don't use 'em, it hardly matters...
 */
NON_EXEC_STATIC void
PelotonMain(int argc, char *argv[])
{
  sigjmp_buf  local_sigjmp_buf;

  am_peloton = true;

  ereport(LOG, (errmsg("starting peloton : pid :: %d", getpid())));

  /* Identify myself via ps */
  init_ps_display("peloton process", "", "", "");

  SetProcessingMode(InitProcessing);

  /*
   * Set up signal handlers.  We operate on databases much like a regular
   * backend, so we use the same signal handling.  See equivalent code in
   * tcop/postgres.c.
   */
  pqsignal(SIGHUP, peloton_sighup_handler);

  /*
   * SIGINT is used to signal canceling the current table's vacuum; SIGTERM
   * means abort and exit cleanly, and SIGQUIT means abandon ship.
   */
  pqsignal(SIGINT, StatementCancelHandler);
  pqsignal(SIGTERM, peloton_sigterm_handler);
  pqsignal(SIGQUIT, quickdie);
  InitializeTimeouts();   /* establishes SIGALRM handler */

  pqsignal(SIGPIPE, SIG_IGN);
  pqsignal(SIGUSR1, peloton_sigusr2_handler);
  pqsignal(SIGUSR2, SIG_IGN);
  pqsignal(SIGFPE, FloatExceptionHandler);
  pqsignal(SIGCHLD, SIG_DFL);

  /* Early initialization */
  BaseInit();

  /*
   * Create a per-backend PGPROC struct in shared memory, except in the
   * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
   * this before we can use LWLocks (and in the EXEC_BACKEND case we already
   * had to do some stuff with LWLocks).
   */
#ifndef EXEC_BACKEND
  InitProcess();
#endif

  /*
   * If an exception is encountered, processing resumes here.
   *
   * See notes in postgres.c about the design of this coding.
   */
  if (sigsetjmp(local_sigjmp_buf, 1) != 0)
  {
    /* Prevents interrupts while cleaning up */
    HOLD_INTERRUPTS();

    /* Report the error to the server log */
    EmitErrorReport();

    /*
     * We can now go away.  Note that because we called InitProcess, a
     * callback was registered to do ProcKill, which will clean up
     * necessary state.
     */
    proc_exit(0);
  }

  /* We can now handle ereport(ERROR) */
  PG_exception_stack = &local_sigjmp_buf;

  PG_SETMASK(&UnBlockSig);

  /*
   * Connect to the selected database
   *
   * Note: if we have selected a just-deleted database (due to using
   * stale stats info), we'll fail and exit here.
   */
  InitPostgres("postgres", InvalidOid, NULL, InvalidOid, NULL);
  SetProcessingMode(NormalProcessing);
  set_ps_display("postgres", false);

  ereport(LOG, (errmsg("peloton: processing database \"%s\"", "postgres")));

  /* Init Peloton */
  InitPeloton("postgres");

  /* Start main loop */
  peloton_MainLoop();

  /* All done, go away */
  proc_exit(0);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
peloton_sighup_handler(SIGNAL_ARGS)
{
  int     save_errno = errno;

  got_SIGHUP = true;
  SetLatch(MyLatch);

  errno = save_errno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void
peloton_sigusr2_handler(SIGNAL_ARGS)
{
  int     save_errno = errno;

  need_exit = true;
  SetLatch(MyLatch);

  errno = save_errno;
}

/* SIGTERM: time to die */
static void
peloton_sigterm_handler(SIGNAL_ARGS)
{
  int     save_errno = errno;

  need_exit = true;
  SetLatch(MyLatch);

  errno = save_errno;
}

/*
 * peloton_MainLoop
 *
 * Main loop for peloton
 */
static void
peloton_MainLoop(void)
{
  int     len;
  Peloton_Msg  msg;
  int     wr;

  /*
   * Loop to process messages until we get SIGQUIT or detect ungraceful
   * death of our parent postmaster.
   *
   * For performance reasons, we don't want to do ResetLatch/WaitLatch after
   * every message; instead, do that only after a recv() fails to obtain a
   * message.  (This effectively means that if backends are sending us stuff
   * like mad, we won't notice postmaster death until things slack off a
   * bit; which seems fine.)  To do that, we have an inner loop that
   * iterates as long as recv() succeeds.  We do recognize got_SIGHUP inside
   * the inner loop, which means that such interrupts will get serviced but
   * the latch won't get cleared until next time there is a break in the
   * action.
   */
  for (;;)
  {
    /* Clear any already-pending wakeups */
    ResetLatch(MyLatch);

    /*
     * Quit if we get SIGQUIT from the postmaster.
     */
    if (need_exit)
      break;

    /*
     * Inner loop iterates as long as we keep getting messages, or until
     * need_exit becomes set.
     */
    while (!need_exit)
    {
      /*
       * Reload configuration if we got SIGHUP from the postmaster.
       */
      if (got_SIGHUP)
      {
        got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
      }

      /*
       * Try to receive and process a message.  This will not block,
       * since the socket is set to non-blocking mode.
       */

      len = recv(pelotonSock, (char *) &msg,
                 sizeof(Peloton_Msg), 0);

      if (len < 0)
      {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
          break;    /* out of inner loop */
        ereport(ERROR,
                (errcode_for_socket_access(),
                    errmsg("could not read statistics message: %m")));
      }

      /*
       * We ignore messages that are smaller than our common header
       */
      if (len < sizeof(Peloton_MsgHdr))
        continue;

      /*
       * The received length must match the length in the header
       */
      if (msg.msg_hdr.m_size != len)
        continue;

      /*
       * O.K. - we accept this message.  Process it.
       */
      switch (msg.msg_hdr.m_type)
      {
        case PELOTON_MTYPE_DUMMY:
          break;

        case PELOTON_MTYPE_PLAN:
          peloton_recv_plan((Peloton_MsgPlan*) &msg, len);
          break;

        default:
          break;
      }
    }           /* end of inner message-processing loop */

    /* Sleep until there's something to do */
    wr = WaitLatchOrSocket(MyLatch,
                           WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE,
                           pelotonSock,
                           -1L);

    /*
     * Emergency bailout if postmaster has died.  This is to avoid the
     * necessity for manual cleanup of all postmaster children.
     */
    if (wr & WL_POSTMASTER_DEATH)
      break;
  }             /* end of outer loop */

  /* Normal exit from peloton is here */
  ereport(LOG,
      (errmsg("peloton shutting down")));

  proc_exit(0);       /* done */
}


/* ----------
 * peloton_init() -
 *
 *  Called from postmaster at startup. Create the resources required
 *  by the peloton process.  If unable to do so, do not
 *  fail --- better to let the postmaster start with peloton
 *  disabled.
 * ----------
 */
void
peloton_init(void)
{
  ACCEPT_TYPE_ARG3 alen;
  struct addrinfo *addrs = NULL,
         *addr,
        hints;
  int     ret;
  fd_set    rset;
  struct timeval tv;
  char    test_byte;
  int     sel_res;
  int     tries = 0;

#define TESTBYTEVAL ((char) 199)

  /*
   * This static assertion verifies that we didn't mess up the calculations
   * involved in selecting maximum payload sizes for our UDP messages.
   * Because the only consequence of overrunning PELOTON_MAX_MSG_SIZE would
   * be silent performance loss from fragmentation, it seems worth having a
   * compile-time cross-check that we didn't.
   */
  //StaticAssertStmt(sizeof(Peloton_Msg) <= PELOTON_MAX_MSG_SIZE,
  //         "maximum peloton message size exceeds PELOTON_MAX_MSG_SIZE");

  /*
   * Create the UDP socket for sending and receiving statistic messages
   */
  hints.ai_flags = AI_PASSIVE;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_protocol = 0;
  hints.ai_addrlen = 0;
  hints.ai_addr = NULL;
  hints.ai_canonname = NULL;
  hints.ai_next = NULL;
  ret = pg_getaddrinfo_all("localhost", NULL, &hints, &addrs);
  if (ret || !addrs)
  {
    ereport(LOG,
        (errmsg("could not resolve \"localhost\": %s",
            gai_strerror(ret))));
    goto startup_failed;
  }

  /*
   * On some platforms, pg_getaddrinfo_all() may return multiple addresses
   * only one of which will actually work (eg, both IPv6 and IPv4 addresses
   * when kernel will reject IPv6).  Worse, the failure may occur at the
   * bind() or perhaps even connect() stage.  So we must loop through the
   * results till we find a working combination. We will generate LOG
   * messages, but no error, for bogus combinations.
   */
  for (addr = addrs; addr; addr = addr->ai_next)
  {
#ifdef HAVE_UNIX_SOCKETS
    /* Ignore AF_UNIX sockets, if any are returned. */
    if (addr->ai_family == AF_UNIX)
      continue;
#endif

    if (++tries > 1)
      ereport(LOG,
      (errmsg("trying another address for the peloton")));

    /*
     * Create the socket.
     */
    if ((pelotonSock = socket(addr->ai_family, SOCK_DGRAM, 0)) == PGINVALID_SOCKET)
    {
      ereport(LOG,
          (errcode_for_socket_access(),
      errmsg("could not create socket for peloton: %m")));
      continue;
    }

    /*
     * Bind it to a kernel assigned port on localhost and get the assigned
     * port via getsockname().
     */
    if (bind(pelotonSock, addr->ai_addr, addr->ai_addrlen) < 0)
    {
      ereport(LOG,
          (errcode_for_socket_access(),
        errmsg("could not bind socket for peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    alen = sizeof(pelotonAddr);
    if (getsockname(pelotonSock, (struct sockaddr *) & pelotonAddr, &alen) < 0)
    {
      ereport(LOG,
          (errcode_for_socket_access(),
           errmsg("could not get address of socket for peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    /*
     * Connect the socket to its own address.  This saves a few cycles by
     * not having to respecify the target address on every send. This also
     * provides a kernel-level check that only packets from this same
     * address will be received.
     */
    if (connect(pelotonSock, (struct sockaddr *) & pelotonAddr, alen) < 0)
    {
      ereport(LOG,
          (errcode_for_socket_access(),
      errmsg("could not connect socket for peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    /*
     * Try to send and receive a one-byte test message on the socket. This
     * is to catch situations where the socket can be created but will not
     * actually pass data (for instance, because kernel packet filtering
     * rules prevent it).
     */
    test_byte = TESTBYTEVAL;

retry1:
    if (send(pelotonSock, &test_byte, 1, 0) != 1)
    {
      if (errno == EINTR)
        goto retry1;  /* if interrupted, just retry */
      ereport(LOG,
          (errcode_for_socket_access(),
           errmsg("could not send test message on socket for peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    /*
     * There could possibly be a little delay before the message can be
     * received.  We arbitrarily allow up to half a second before deciding
     * it's broken.
     */
    for (;;)        /* need a loop to handle EINTR */
    {
      FD_ZERO(&rset);
      FD_SET(pelotonSock, &rset);

      tv.tv_sec = 0;
      tv.tv_usec = 500000;
      sel_res = select(pelotonSock + 1, &rset, NULL, NULL, &tv);
      if (sel_res >= 0 || errno != EINTR)
        break;
    }
    if (sel_res < 0)
    {
      ereport(LOG,
          (errcode_for_socket_access(),
           errmsg("select() failed in peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }
    if (sel_res == 0 || !FD_ISSET(pelotonSock, &rset))
    {
      /*
       * This is the case we actually think is likely, so take pains to
       * give a specific message for it.
       *
       * errno will not be set meaningfully here, so don't use it.
       */
      ereport(LOG,
          (errcode(ERRCODE_CONNECTION_FAILURE),
           errmsg("test message did not get through on socket for peloton")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    test_byte++;      /* just make sure variable is changed */

retry2:
    if (recv(pelotonSock, &test_byte, 1, 0) != 1)
    {
      if (errno == EINTR)
        goto retry2;  /* if interrupted, just retry */
      ereport(LOG,
          (errcode_for_socket_access(),
           errmsg("could not receive test message on socket for peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    if (test_byte != TESTBYTEVAL) /* strictly paranoia ... */
    {
      ereport(LOG,
          (errcode(ERRCODE_INTERNAL_ERROR),
           errmsg("incorrect test message transmission on socket for peloton")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    /* If we get here, we have a working socket */
    break;
  }

  /* Did we find a working address? */
  if (!addr || pelotonSock == PGINVALID_SOCKET)
    goto startup_failed;

  /*
   * Set the socket to non-blocking IO.  This ensures that if the collector
   * falls behind, statistics messages will be discarded; backends won't
   * block waiting to send messages to the collector.
   */
  if (!pg_set_noblock(pelotonSock))
  {
    ereport(LOG,
        (errcode_for_socket_access(),
         errmsg("could not set peloton socket to nonblocking mode: %m")));
    goto startup_failed;
  }

  pg_freeaddrinfo_all(hints.ai_family, addrs);

  return;

startup_failed:
  ereport(LOG,
    (errmsg("disabling peloton for lack of working socket")));

  if (addrs)
    pg_freeaddrinfo_all(hints.ai_family, addrs);

  if (pelotonSock != PGINVALID_SOCKET)
    closesocket(pelotonSock);
  pelotonSock = PGINVALID_SOCKET;

  /*
   * Adjust GUC variables to suppress useless activity, and for debugging
   * purposes (seeing track_counts off is a clue that we failed here). We
   * use PGC_S_OVERRIDE because there is no point in trying to turn it back
   * on from postgresql.conf without a restart.
   */
  SetConfigOption("track_counts", "off", PGC_INTERNAL, PGC_S_OVERRIDE);
}


/* ------------------------------------------------------------
 * Local support functions follow
 * ------------------------------------------------------------
 */

/* ----------
 * peloton_setheader() -
 *
 *    Set common header fields in a peloton message
 * ----------
 */
static void
peloton_setheader(Peloton_MsgHdr *hdr, PelotonMsgType mtype)
{
  hdr->m_type = mtype;
}


/* ----------
 * peloton_send() -
 *
 *    Send out one peloton message to the collector
 * ----------
 */
static void
peloton_send(void *msg, int len)
{
  int     rc;

  if (pelotonSock == PGINVALID_SOCKET)
    return;

  ((Peloton_MsgHdr *) msg)->m_size = len;

  /* We'll retry after EINTR, but ignore all other failures */
  do
  {
    rc = send(pelotonSock, msg, len, 0);
  } while (rc < 0 && errno == EINTR);

#ifdef USE_ASSERT_CHECKING
  /* In debug builds, log send failures ... */
  if (rc < 0)
    elog(LOG, "could not send to peloton: %m");
#endif
}

/* ----------
 * peloton_send_ping() -
 *
 *  Send some junk data to the collector to increase traffic.
 * ----------
 */
void
peloton_send_ping(void)
{
  Peloton_MsgDummy msg;

  if (pelotonSock == PGINVALID_SOCKET)
    return;

  peloton_setheader(&msg.m_hdr, PELOTON_MTYPE_DUMMY);
  peloton_send(&msg, sizeof(msg));
}

/* ----------
 * peloton_send_node() -
 *
 *  Execute the given node to return a(nother) tuple.
 * ----------
 */
void
peloton_send_node(PlanState *node)
{
  Peloton_MsgPlan msg;
  MemoryContext oldcontext;

  if (pelotonSock == PGINVALID_SOCKET)
    return;

  peloton_setheader(&msg.m_hdr, PELOTON_MTYPE_PLAN);

  /*
   * Switch to TopSharedMemoryContext context for copying plan.
   */
  oldcontext = MemoryContextSwitchTo(TopSharedMemoryContext);

  // TODO: Can we avoid copying the plan ?
  msg.m_node = (PlanState *) palloc(sizeof(PlanState));
  memcpy(msg.m_node, node, sizeof(PlanState));

  /*
   * Switch back to old context.
   */
  MemoryContextSwitchTo(oldcontext);

  peloton_send(&msg, sizeof(msg));
}

/* ----------
 * peloton_recv_plan() -
 *
 *  Process plan execution requests.
 * ----------
 */
static void
peloton_recv_plan(Peloton_MsgPlan *msg, int len)
{
  PlanState *node;
  Plan *plan;

  if(msg != NULL)
  {
    /* Get the planstate */
    node = msg->m_node;

    if(node != NULL)
    {
      /* Get the plan */
      plan = node->plan;

      fprintf(stdout, "Planstate type : %d\n", plan->type);
      //elog_node_display(LOG, "plan", plan, Debug_pretty_print);
    }
  }

  /* Print stats */
  //SHMContextStats(TopSharedMemoryContext);
}

