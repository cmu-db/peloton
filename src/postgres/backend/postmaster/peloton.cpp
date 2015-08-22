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
#include <thread>
#include <map>

#include "backend/common/logger.h"
#include "backend/common/message_queue.h"
#include "backend/scheduler/tbb_scheduler.h"
#include "backend/bridge/ddl/configuration.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_utils.h"
#include "backend/bridge/ddl/tests/bridge_test.h"
#include "backend/bridge/dml/executor/plan_executor.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/common/stack_trace.h"
#include "backend/logging/logmanager.h"

#include "postgres.h"
#include "c.h"
#include "access/xact.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "catalog/pg_namespace.h"
#include "executor/tuptable.h"
#include "libpq/ip.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"

#include "nodes/params.h"
#include "utils/guc.h"
#include "utils/errcodes.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/peloton.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"


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

/* Peloton map to keep track of backend queues */
std::map<BackendId, mqd_t> backend_queue_map;
std::mutex backend_queue_map_mutex;

/* ----------
 * Local function forward declarations
 * ----------
 */
NON_EXEC_STATIC void PelotonMain(int argc, char *argv[]) pg_attribute_noreturn();
static void peloton_MainLoop(void);
static void peloton_sighup_handler(SIGNAL_ARGS);
static void peloton_sigusr2_handler(SIGNAL_ARGS);
static void peloton_sigterm_handler(SIGNAL_ARGS);
static void peloton_sighup_handler(SIGNAL_ARGS);
static void peloton_sigsegv_handler(SIGNAL_ARGS);
static void peloton_sigabrt_handler(SIGNAL_ARGS);

static void peloton_setheader(Peloton_MsgHdr *hdr,
                              PelotonMsgType mtype,
                              BackendId m_backend_id,
                              Oid MyDatabaseId,
                              TransactionId txn_id,
                              MemoryContext query_context);

static void peloton_send(void *msg, int len);

static void peloton_process_dml(Peloton_MsgDML *msg);
static void peloton_process_ddl(Peloton_MsgDDL *msg);
static void peloton_process_bootstrap(Peloton_MsgBootstrap *msg);

static void __attribute__((unused)) peloton_test_config();

static Peloton_Status *peloton_create_status();
static void peloton_process_status(Peloton_Status *status);
static void peloton_destroy_status(Peloton_Status *status);

static void __attribute__((unused)) peloton_update_stats(Peloton_Status *status);
static void peloton_reply_to_backend(mqd_t backend_id);

static void peloton_send_output(Peloton_Status  *status,
                                bool sendTuples,
                                DestReceiver *dest);

static Node *peloton_copy_parsetree(Node *parsetree);
static ParamListInfo peloton_copy_paramlist(ParamListInfo param_list);

static void peloton_begin_query();
static void __attribute__((unused)) peloton_finish_query();


bool
IsPelotonProcess(void) {
  return am_peloton;
}

/**
 * @brief Initialize peloton
 */
int peloton_start(void) {
  pid_t   pelotonPid;

  switch ((pelotonPid = fork_process())) {
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

      /*
       * Make sure we aren't in PostmasterContext anymore.  (We can't delete it
       * just yet, though, because InitPostgres will need the HBA data.)
       */
      MemoryContextSwitchTo(TopMemoryContext);

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
PelotonMain(int argc, char *argv[]) {

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
  pqsignal(SIGSEGV, peloton_sigsegv_handler);
  pqsignal(SIGABRT, peloton_sigabrt_handler);
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
  if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
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
   * Connect to the test database for Peloton Test Mode
   */
  if(PelotonTestMode == true) {
    InitPostgres("postgres", InvalidOid, NULL, InvalidOid, NULL);
  }

  /*
   * If the PostmasterContext is still around, recycle the space; we don't
   * need it anymore after InitPostgres completes.  Note this does not trash
   * *MyProcPort, because ConnCreate() allocated that space with malloc()
   * ... else we'd need to copy the Port data first.  Also, subsidiary data
   * such as the username isn't lost either; see ProcessStartupPacket().
   */
  if (PostmasterContext) {
    MemoryContextDelete(PostmasterContext);
    PostmasterContext = NULL;
  }

  SetProcessingMode(NormalProcessing);

  // Disable stacktracer for now
  //peloton::StackTracer st;

  /*
   * Create the memory context we will use in the main loop.
   *
   * MessageContext is reset once per iteration of the main loop, ie, upon
   * completion of processing of each command message from the client.
   */
  MessageContext = AllocSetContextCreate(TopMemoryContext,
                                         "MessageContext",
                                         ALLOCSET_DEFAULT_MINSIZE,
                                         ALLOCSET_DEFAULT_INITSIZE,
                                         ALLOCSET_DEFAULT_MAXSIZE);

  ereport(LOG, (errmsg("peloton: processing database \"%s\"", "postgres")));

  /*
   * Create a resource owner to keep track of our resources (not clear that
   * we need this, but may as well have one).
   */
  CurrentResourceOwner = ResourceOwnerCreate(NULL, "Peloton");

  /*
   * Make sure we aren't in PostmasterContext anymore.  (We can't delete it
   * just yet, though, because InitPostgres will need the HBA data.)
   */
  MemoryContextSwitchTo(TopMemoryContext);

  /* Testing mode */
  if(PelotonTestMode == true) {
    peloton::bridge::BridgeTest::RunTests();
  }

  /* Start main loop */
  peloton_MainLoop();

  // TODO: Peloton Changes
  MemoryContextDelete(MessageContext);
  MemoryContextDelete(CacheMemoryContext);

  /* All done, go away */
  proc_exit(0);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
peloton_sighup_handler(SIGNAL_ARGS) {
  int     save_errno = errno;

  got_SIGHUP = true;
  SetLatch(MyLatch);

  errno = save_errno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void
peloton_sigusr2_handler(SIGNAL_ARGS) {
  int     save_errno = errno;

  need_exit = true;
  SetLatch(MyLatch);

  errno = save_errno;
}

/* SIGTERM: time to die */
static void
peloton_sigterm_handler(SIGNAL_ARGS) {
  int     save_errno = errno;

  need_exit = true;
  SetLatch(MyLatch);

  errno = save_errno;
}

/* SIGSEGV: time to die */
static void
peloton_sigsegv_handler(SIGNAL_ARGS) {
  int     save_errno = errno;

  need_exit = true;
  SetLatch(MyLatch);

  // Get stack trace
  peloton::GetStackTrace();

  errno = save_errno;

  // We can now go away.
  proc_exit(0);
}

/* SIGABRT: time to die */
static void
peloton_sigabrt_handler(SIGNAL_ARGS) {
  int     save_errno = errno;

  need_exit = true;
  SetLatch(MyLatch);

  // Get stack trace
  peloton::GetStackTrace();

  errno = save_errno;

  // We can now go away.
  proc_exit(0);
}

/*
 * peloton_MainLoop
 *
 * Main loop for peloton
 */
static void
peloton_MainLoop(void) {
  int     len;
  Peloton_Msg  msg;
  int     wr;

  // Launching a thread for logging 
//  auto& logManager = peloton::logging::LogManager::GetInstance();
//  logManager.SetMainLoggingType(peloton::LOGGING_TYPE_ARIES);
//  std::thread thread(&peloton::logging::LogManager::StandbyLogging,
//                     &logManager,
//                     logManager.GetMainLoggingType());

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
  for (;;) {
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
    while (!need_exit) {
      /*
       * Reload configuration if we got SIGHUP from the postmaster.
       */
      if (got_SIGHUP) {
        got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
      }

      /*
       * Try to receive and process a message.  This will not block,
       * since the socket is set to non-blocking mode.
       */

      len = recv(pelotonSock, (char *) &msg,
                 sizeof(Peloton_Msg), 0);

      if (len < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
          break;    /* out of inner loop */
        ereport(ERROR,
                (errcode_for_socket_access(),
                    errmsg("could not read message from backend to peloton: %m")));
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
      switch (msg.msg_hdr.m_type) {
        case PELOTON_MTYPE_DUMMY:
          break;

        case PELOTON_MTYPE_DDL: {
          std::thread(peloton_process_ddl, reinterpret_cast<Peloton_MsgDDL*>(&msg)).detach();
        }
        break;

        case PELOTON_MTYPE_DML: {
          std::thread(peloton_process_dml, reinterpret_cast<Peloton_MsgDML*>(&msg)).detach();
        }
        break;

        case PELOTON_MTYPE_BOOTSTRAP: {
          std::thread(peloton_process_bootstrap, reinterpret_cast<Peloton_MsgBootstrap*>(&msg)).detach();
        }
        break;

        default:
          break;
      }
    }  /* end of inner message-processing loop */

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

  // terminate logging thread
//  if( logManager.EndLogging() ){
//    thread.join();
//  }else{
//    elog(LOG,"Failed to terminate logging thread");
//  }

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
peloton_init(void) {
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
  if (ret || !addrs) {
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
  for (addr = addrs; addr; addr = addr->ai_next) {
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
    if ((pelotonSock = socket(addr->ai_family, SOCK_DGRAM, 0)) == PGINVALID_SOCKET) {
      ereport(LOG,
              (errcode_for_socket_access(),
                  errmsg("could not create socket for peloton: %m")));
      continue;
    }

    /*
     * Bind it to a kernel assigned port on localhost and get the assigned
     * port via getsockname().
     */
    if (bind(pelotonSock, addr->ai_addr, addr->ai_addrlen) < 0) {
      ereport(LOG,
              (errcode_for_socket_access(),
                  errmsg("could not bind socket for peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    alen = sizeof(pelotonAddr);
    if (getsockname(pelotonSock, (struct sockaddr *) & pelotonAddr, &alen) < 0) {
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
    if (connect(pelotonSock, (struct sockaddr *) & pelotonAddr, alen) < 0) {
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
    if (send(pelotonSock, &test_byte, 1, 0) != 1) {
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
    for (;;) {       /* need a loop to handle EINTR */
      FD_ZERO(&rset);
      FD_SET(pelotonSock, &rset);

      tv.tv_sec = 0;
      tv.tv_usec = 500000;
      sel_res = select(pelotonSock + 1, &rset, NULL, NULL, &tv);
      if (sel_res >= 0 || errno != EINTR)
        break;
    }
    if (sel_res < 0) {
      ereport(LOG,
              (errcode_for_socket_access(),
                  errmsg("select() failed in peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }
    if (sel_res == 0 || !FD_ISSET(pelotonSock, &rset)) {
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
    if (recv(pelotonSock, &test_byte, 1, 0) != 1) {
      if (errno == EINTR)
        goto retry2;  /* if interrupted, just retry */
      ereport(LOG,
              (errcode_for_socket_access(),
                  errmsg("could not receive test message on socket for peloton: %m")));
      closesocket(pelotonSock);
      pelotonSock = PGINVALID_SOCKET;
      continue;
    }

    if (test_byte != TESTBYTEVAL) { /* strictly paranoia ... */
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
  if (!pg_set_noblock(pelotonSock)) {
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
peloton_setheader(Peloton_MsgHdr *hdr,
                  PelotonMsgType mtype,
                  BackendId m_backend_id,
                  Oid MyDatabaseId,
                  TransactionId txn_id,
                  MemoryContext query_context) {
  hdr->m_type = mtype;
  hdr->m_backend_id = m_backend_id;
  hdr->m_dbid = MyDatabaseId;
  hdr->m_txn_id = txn_id;
  hdr->m_query_context = SHMQueryContext;
}


/* ----------
 * peloton_send() -
 *
 *    Send out one peloton message to the collector
 * ----------
 */
static void
peloton_send(void *msg, int len) {
  int     rc;

  if (pelotonSock == PGINVALID_SOCKET)
    return;

  ((Peloton_MsgHdr *) msg)->m_size = len;

  /* We'll retry after EINTR, but ignore all other failures */
  do {
    rc = send(pelotonSock, msg, len, 0);
  } while (rc < 0 && errno == EINTR);

#ifdef USE_ASSERT_CHECKING
  /* In debug builds, log send failures ... */
  if (rc < 0)
    elog(LOG, "could not send to peloton: %m");
#endif
}

/* ----------
 * peloton_switch_query_context() -
 *
 *  Create and switch to query context.
 * ----------
 */

void
peloton_begin_query() {

  // First, create a new query subcontext
  SHMQueryContext = SHMAllocSetContextCreate(TopSharedMemoryContext,
                                             "SHM Query Subcontext",
                                             ALLOCSET_DEFAULT_MINSIZE,
                                             ALLOCSET_DEFAULT_INITSIZE,
                                             ALLOCSET_DEFAULT_MAXSIZE,
                                             SHM_DEFAULT_SEGMENT);

}

/* ----------
 * peloton_send_output() -
 *
 *  Send the output to the receiver.
 * ----------
 */
void
peloton_send_output(Peloton_Status  *status,
                    bool sendTuples,
                    DestReceiver *dest) {
  TupleTableSlot *slot;

  // Go over any result slots
  if(status->m_result_slots != NULL)  {
    ListCell   *lc;

    foreach(lc, status->m_result_slots)
    {
      slot = (TupleTableSlot *) lfirst(lc);

      /*
       * if the tuple is null, then we assume there is nothing more to
       * process so we just end the loop...
       */
      if (TupIsNull(slot))
        break;

      /*
       * If we are supposed to send the tuple somewhere, do so. (In
       * practice, this is probably always the case at this point.)
       */
      if (sendTuples)
        (*dest->receiveSlot) (slot, dest);

      /*
       * Free the underlying heap_tuple
       * and the TupleTableSlot itself.
       */
      ExecDropSingleTupleTableSlot(slot);
    }

    // Clean up list
    list_free(status->m_result_slots);
  }

}

/* ----------
 * peloton_send_dml() -
 *
 *  Execute the given node to return a(nother) tuple.
 * ----------
 */
void
peloton_send_dml(PlanState *planstate,
                 bool sendTuples,
                 DestReceiver *dest,
                 TupleDesc tuple_desc) {
  Peloton_Status  *status;
  Peloton_MsgDML msg;
  MemoryContext oldcxt;
  assert(pelotonSock != PGINVALID_SOCKET);

  // Set header
  auto transaction_id = GetTopTransactionId();

  peloton_setheader(&msg.m_hdr,
                    PELOTON_MTYPE_DML,
                    MyBackendId,
                    MyDatabaseId,
                    transaction_id,
                    SHMQueryContext);

  // Create and switch to the query context
  peloton_begin_query();

  oldcxt = MemoryContextSwitchTo(SHMQueryContext);

  status = peloton_create_status();
  msg.m_status = status;

  // Copy the tuple desc
  msg.m_tuple_desc = CreateTupleDescCopy(tuple_desc);
  elog(INFO, "Copied tuple desc : %p", msg.m_tuple_desc);

  // Copy the param list
  assert(planstate != NULL);
  assert(planstate->state != NULL);

  auto param_list = planstate->state->es_param_list_info;
  auto shm_param_list = peloton_copy_paramlist(param_list);
  msg.m_param_list = shm_param_list;

  // Create the raw planstate info
  auto plan_state = peloton::bridge::DMLUtils::peloton_prepare_data(planstate);
  msg.m_plan_state = plan_state;

  MemoryContextSwitchTo(oldcxt);

  peloton_send(&msg, sizeof(msg));

  // Wait for the response and process it
  peloton_process_status(status);

  // Send output to dest
  peloton_send_output(status, sendTuples, dest);

  peloton_destroy_status(status);

  // Do final cleanup
  peloton_finish_query();

}

/* ----------
 * peloton_send_ddl -
 *
 *  Send DDL requests to Peloton.
 * ----------
 */
void
peloton_send_ddl(Node *parsetree) {
  Peloton_Status  *status;
  Peloton_MsgDDL msg;
  MemoryContext oldcxt;
  assert(pelotonSock != PGINVALID_SOCKET);

  // Set header
  auto transaction_id = GetTopTransactionId();

  peloton_setheader(&msg.m_hdr,
                    PELOTON_MTYPE_DDL,
                    MyBackendId,
                    MyDatabaseId,
                    transaction_id,
                    SHMQueryContext);

  // Create and switch to the query context
  peloton_begin_query();

  oldcxt = MemoryContextSwitchTo(SHMQueryContext);

  status = peloton_create_status();
  msg.m_status = status;

  auto ddl_info = peloton::bridge::DDLUtils::peloton_prepare_data(parsetree);

  auto parsetree_copy = peloton_copy_parsetree(parsetree);
  msg.m_parsetree = parsetree_copy;
  msg.m_ddl_info = ddl_info;

  // Switch back to old context
  MemoryContextSwitchTo(oldcxt);

  peloton_send(&msg, sizeof(msg));

  // Wait for the response and process it
  peloton_process_status(status);

  peloton_destroy_status(status);

  // Do final cleanup
  peloton_finish_query();

}

/* ----------
 * peloton_send_bootstrap -
 *
 *  Send bootstrap requests to Peloton.
 * ----------
 */
void
peloton_send_bootstrap(){
  Peloton_Status  *status;
  Peloton_MsgBootstrap msg;
  MemoryContext oldcxt;
  assert(pelotonSock != PGINVALID_SOCKET);

  // Set header
  auto transaction_id = GetTopTransactionId();

  peloton_setheader(&msg.m_hdr,
                    PELOTON_MTYPE_BOOTSTRAP,
                    MyBackendId,
                    MyDatabaseId,
                    transaction_id,
                    SHMQueryContext);

  // Create and switch to the query context
  peloton_begin_query();

  oldcxt = MemoryContextSwitchTo(SHMQueryContext);

  status = peloton_create_status();
  msg.m_status = status;

  // Construct raw database for bootstrap
  raw_database_info* raw_database = peloton::bridge::Bootstrap::GetRawDatabase();
  msg.m_raw_database = raw_database;

  MemoryContextSwitchTo(oldcxt);

  peloton_send(&msg, sizeof(msg));

  // Wait for the response and process it
  peloton_process_status(status);

  peloton_destroy_status(status);

  // Do final cleanup
  peloton_finish_query();

}

/* ----------
 * peloton_finish_query -
 *
 *  Cleanup
 * ----------
 */
static void
peloton_finish_query() {

  // First, clean up the query context
  if(SHMQueryContext != NULL)
    SHMContextDelete(SHMQueryContext);

}


/* ----------
 * peloton_reply_to_backend -
 *
 *  Send reply back to backend
 * ----------
 */
static void
peloton_reply_to_backend(mqd_t backend_id) {
  /*
  mqd_t mqd = -1;

  // Access queue map
  {
    std::lock_guard<std::mutex> lock(backend_queue_map_mutex);

    // Check if we already have opened the queue
    if(backend_queue_map.count(backend_id) != 0) {
      mqd = backend_queue_map[backend_id];
    }
    else {
      auto queue_name = peloton::get_mq_name(backend_id);
      mqd = peloton::open_mq(queue_name);
      backend_queue_map[backend_id] = mqd;
    }
  }

  // Send some message
  peloton::send_message(mqd, "test_msg");
  */
}

/* ----------
 * peloton_process_dml -
 *
 *  Process DML requests in Peloton.
 * ----------
 */
static void
peloton_process_dml(Peloton_MsgDML *msg) {
  assert(msg);

  /* Get the planstate */
  auto planstate = msg->m_plan_state;

  auto plan = peloton::bridge::PlanTransformer::TransformPlan(planstate);

  /* Ignore empty plans */
  if(plan == nullptr) {
    elog(WARNING, "Empty or unrecognized plan is sent to Peloton");
    msg->m_status->m_result =  peloton::RESULT_FAILURE;
    peloton_reply_to_backend(msg->m_hdr.m_backend_id);
  }

  MyDatabaseId = msg->m_hdr.m_dbid;
  TransactionId txn_id = msg->m_hdr.m_txn_id;
  SHMQueryContext = msg->m_hdr.m_query_context;

  ParamListInfo param_list = msg->m_param_list;
  TupleDesc tuple_desc = msg->m_tuple_desc;

  try {
    // Execute the plantree
    peloton::bridge::PlanExecutor::ExecutePlan(plan,
                                               param_list,
                                               tuple_desc,
                                               msg->m_status,
                                               txn_id);

    // Clean up the plantree
    peloton::bridge::PlanTransformer::CleanPlan(plan);
  }
  catch(const std::exception &exception) {
    elog(ERROR, "Peloton exception :: %s", exception.what());

    peloton::GetStackTrace();

    msg->m_status->m_result = peloton::RESULT_FAILURE;
  }

  // Send reply
  peloton_reply_to_backend(msg->m_hdr.m_backend_id);
}

/* ----------
 * peloton_process_ddl() -
 *
 *  Process DDL requests in Peloton.
 * ----------
 */
static void
peloton_process_ddl(Peloton_MsgDDL *msg) {
  Node* parsetree;
  assert(msg);

  /* Get the parsetree */
  parsetree = msg->m_parsetree;
  auto ddl_info = msg->m_ddl_info;

  /* Ignore invalid parsetrees */
  if(parsetree == NULL || nodeTag(parsetree) == T_Invalid) {
    msg->m_status->m_result =  peloton::RESULT_FAILURE;
    peloton_reply_to_backend(msg->m_hdr.m_backend_id);
  }

  MyDatabaseId = msg->m_hdr.m_dbid;
  TransactionId txn_id = msg->m_hdr.m_txn_id;
  SHMQueryContext = msg->m_hdr.m_query_context;

  try {
    /* Process the utility statement */
    peloton::bridge::DDL::ProcessUtility(parsetree,
                                         ddl_info,
                                         msg->m_status,
                                         txn_id);
  }
  catch(const std::exception &exception) {
    elog(ERROR, "Peloton exception :: %s", exception.what());
    // Nothing to do here !
  }

  // Set Status
  msg->m_status->m_result = peloton::RESULT_SUCCESS;
  peloton_reply_to_backend(msg->m_hdr.m_backend_id);
}

/* ----------
 * peloton_process_bootstrap() -
 *
 *  Process Bootstrap requests in Peloton.
 * ----------
 */
static void
peloton_process_bootstrap(Peloton_MsgBootstrap *msg) {
  assert(msg);

  /* Get the raw databases */
  raw_database_info* raw_database = msg->m_raw_database;

  /* Ignore invalid parsetrees */
  if(raw_database == NULL) {
    msg->m_status->m_result =  peloton::RESULT_FAILURE;
    peloton_reply_to_backend(msg->m_hdr.m_backend_id);
  }

  MyDatabaseId = msg->m_hdr.m_dbid;
  SHMQueryContext = msg->m_hdr.m_query_context;

  if(raw_database != NULL) {
    try {
      /* Process the utility statement */
      peloton::bridge::Bootstrap::BootstrapPeloton(raw_database,
                                                   msg->m_status);

      // NOTE:: start logging since bootstrapPeloton is done
      auto& logManager = peloton::logging::LogManager::GetInstance();
      if( logManager.IsReadyToLogging() == false){
        logManager.StartLogging();
      }
    }
    catch(const std::exception &exception) {
      elog(ERROR, "Peloton exception :: %s", exception.what());
      // Nothing to do here !
    }
  }

  // Set Status
  msg->m_status->m_result = peloton::RESULT_SUCCESS;
  peloton_reply_to_backend(msg->m_hdr.m_backend_id);
}

/* ----------
 * peloton_create_status() -
 *
 *  Allocate and initialize status space.
 * ----------
 */
Peloton_Status *
peloton_create_status() {
  Peloton_Status *status = (Peloton_Status *) palloc(sizeof(Peloton_Status));
  assert(status != NULL);

  status->m_result = peloton::RESULT_INVALID;
  status->m_result_slots = NULL;
  status->m_status = -1;
  status->m_dirty_count = 0; 

  return status;
}

/* ----------
 * peloton_process_status() -
 *
 *  Busy wait till we get status from Peloton.
 * ----------
 */
void
peloton_process_status(Peloton_Status *status) {
  int code;
  struct timespec duration = {0, 100}; // 100 us
  int rc;

  assert(status);

  // Busy wait till we get a valid result
  while(status->m_result == peloton::RESULT_INVALID) {
    rc = nanosleep(&duration, NULL);
    if(rc < 0) {
      break;
    }

    /* additive increase */
    duration.tv_nsec += 100;
    //elog(DEBUG2, "Busy waiting");
  }

  //peloton::wait_for_message(&MyBackendQueue);

  // Process the status code
  code = status->m_result;
  switch(code) {
    case peloton::RESULT_SUCCESS: {
      // check dirty bit to see if we need to update stats
      if(status->m_dirty_count){
        //peloton_update_stats(status);
      }
    }
    break;

    case peloton::RESULT_INVALID:
    case peloton::RESULT_FAILURE:
    default: {
      ereport(ERROR, (errcode(status->m_status),
          errmsg("transaction failed")));
    }
    break;
  }

}

/* ----------
 * peloton_destroy_status() -
 *
 *  Deallocate status
 * ----------
 */
void
peloton_destroy_status(Peloton_Status *status) {
  pfree(status);
}

static void
peloton_test_config() {

  auto val = GetConfigOption("peloton_mode", false, false);
  elog(LOG, "Before SetConfigOption : %s", val);

  SetConfigOption("peloton_mode", "peloton_mode_1", PGC_USERSET, PGC_S_USER);

  val = GetConfigOption("peloton_mode", false, false);
  elog(LOG, "After SetConfigOption : %s", val);

  // Build the configuration map
  peloton::bridge::ConfigManager::BuildConfigMap();
}

/* ----------
 * IsPelotonQuery -
 *
 *  Does the query access peloton tables or not ?
 * ----------
 */
bool IsPelotonQuery(List *relationOids) {
  bool peloton_query = false;

  // Check if we are in Postmaster environment */
  if(IsPostmasterEnvironment == false)
    return false;

  if(relationOids != NULL) {
    ListCell   *lc;

    // Go over each relation on which the plan depends
    foreach(lc, relationOids) {
      Oid relationOid = lfirst_oid(lc);
      // Fast check to determine if the relation is a peloton relation
      if(relationOid >= FirstNormalObjectId) {
        peloton_query = true;
        break;
      }
    }
  }

  return peloton_query;
}

static void
peloton_update_stats(Peloton_Status *status) {

  int dirty_table_count = status->m_dirty_count;

  // Go over each dirty table and update stats
  // This is executed with Backend
  for(int table_itr=0; table_itr<dirty_table_count; table_itr++){
    auto dirty_table = status->m_dirty_tables[table_itr];
    peloton::bridge::Bridge::SetNumberOfTuples(dirty_table->table_oid,
                                               dirty_table->number_of_tuples);

    int dirty_index_count = dirty_table->dirty_index_count;
    // Go over each index within the table
    for(int index_itr=0; index_itr<dirty_index_count; index_itr++){
      auto dirty_index = dirty_table->dirty_indexes[index_itr];
      peloton::bridge::Bridge::SetNumberOfTuples(dirty_index->index_oid,
                                                 dirty_index->number_of_tuples);
    }

  }

}

static
Node *peloton_copy_parsetree(Node *parsetree) {
  // Copy the parsetree
  Node     *shm_parsetree = (Node *) copyObject(parsetree);
  return shm_parsetree;
}

static
ParamListInfo peloton_copy_paramlist(ParamListInfo param_list) {
  // Copy the parsetree
  auto shm_param_list = copyParamList(param_list);
  return shm_param_list;
}

