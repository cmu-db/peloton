/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/libpq-be.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "storage/backendid.h"

// TODO: Peloton Changes
#include "backend/common/message_queue.h"

thread_local ProtocolVersion FrontendProtocol;

thread_local volatile bool InterruptPending = false;
thread_local volatile bool QueryCancelPending = false;
thread_local volatile bool ProcDiePending = false;
thread_local volatile bool ClientConnectionLost = false;
thread_local volatile uint32 InterruptHoldoffCount = 0;
thread_local volatile uint32 QueryCancelHoldoffCount = 0;
thread_local volatile uint32 CritSectionCount = 0;

thread_local int			MyProcPid;
thread_local pg_time_t	MyStartTime;
thread_local struct Port *MyProcPort;
thread_local long		MyCancelKey;
thread_local int			MyPMChildSlot;

/*
 * MyLatch points to the latch that should be used for signal handling by the
 * current process. It will either point to a process local latch if the
 * current process does not have a PGPROC entry in that moment, or to
 * PGPROC->procLatch if it has. Thus it can always be used in signal handlers,
 * without checking for its existence.
 */
thread_local struct Latch *MyLatch;

/*
 * DataDir is the absolute path to the top level of the PGDATA directory tree.
 * Except during early startup, this is also the server's working directory;
 * most code therefore can simply use relative paths and not reference DataDir
 * explicitly.
 */
thread_local char	   *DataDir = NULL;

thread_local char		OutputFileName[MAXPGPATH];	/* debugging output file */

thread_local char		my_exec_path[MAXPGPATH];	/* full path to my executable */
thread_local char		pkglib_path[MAXPGPATH];		/* full path to lib directory */

#ifdef EXEC_BACKEND
char		postgres_exec_path[MAXPGPATH];		/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

thread_local BackendId	MyBackendId = InvalidBackendId;

thread_local mqd_t MyBackendQueue = InvalidBackendId;

thread_local Oid  MyDatabaseId = InvalidOid;

thread_local Oid			MyDatabaseTableSpace = InvalidOid;

/*
 * DatabasePath is the path (relative to DataDir) of my database's
 * primary directory, ie, its directory in the default tablespace.
 */
thread_local char	   *DatabasePath = NULL;

thread_local pid_t		PostmasterPid = 0;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
thread_local bool		IsPostmasterEnvironment = false;
thread_local bool		IsUnderPostmaster = false;
thread_local bool		IsBinaryUpgrade = false;
thread_local bool		IsBackgroundWorker = false;
thread_local bool   IsBackend = false;

thread_local bool		ExitOnAnyError = false;

int			DateStyle = USE_ISO_DATES;
int			DateOrder = DATEORDER_MDY;
int			IntervalStyle = INTSTYLE_POSTGRES;

bool		enableFsync = true;
bool		allowSystemTableMods = false;
int			work_mem = 1024;
int			maintenance_work_mem = 16384;

/*
 * Primary determinants of sizes of shared-memory structures.
 *
 * MaxBackends is computed by PostmasterMain after modules have had a chance to
 * register background workers.
 */
int			NBuffers = 1000;
int			MaxConnections = 90;
int			max_worker_processes = 8;
thread_local int			MaxBackends = 0;

int			VacuumCostPageHit = 1;		/* GUC parameters for vacuum */
int			VacuumCostPageMiss = 10;
int			VacuumCostPageDirty = 20;
int			VacuumCostLimit = 200;
int			VacuumCostDelay = 0;

int			VacuumPageHit = 0;
int			VacuumPageMiss = 0;
int			VacuumPageDirty = 0;

int			VacuumCostBalance = 0;		/* working state for vacuum */
bool		VacuumCostActive = false;
