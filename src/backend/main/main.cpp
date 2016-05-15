//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// main.cpp
//
// Identification: src/backend/main/main.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include "postgres/include/postgres.h"

#include "postgres/include/bootstrap/bootstrap.h"
#include "postgres/include/common/username.h"
#include "postgres/include/postmaster/postmaster.h"
#include "postgres/include/storage/barrier.h"
#include "postgres/include/storage/s_lock.h"
#include "postgres/include/storage/spin.h"
#include "postgres/include/tcop/tcopprot.h"
#include "postgres/include/utils/help_config.h"
#include "postgres/include/utils/memutils.h"
#include "postgres/include/utils/pg_locale.h"
#include "postgres/include/utils/ps_status.h"

#include "backend/common/macros.h"

const char *progname;

static void startup_hacks(const char *progname);
static void init_locale(int category, const char *locale);
static void help(const char *progname);
static void check_root(const char *progname);

/*
 * Any Postgres server process begins execution here.
 */
int main(int argc, char *argv[]) {
  bool do_check_root = true;

  progname = "peloton";
  // progname = get_progname(argv[0]);

  /*
   * Platform-specific startup hacks
   */
  startup_hacks(progname);

  /*
   * Remember the physical location of the initially given argv[] array for
   * possible use by ps display.  On some platforms, the argv[] storage must
   * be overwritten in order to set the process title for ps. In such cases
   * save_ps_display_args makes and returns a new copy of the argv[] array.
   *
   * save_ps_display_args may also move the environment strings to make
   * extra room. Therefore this should be done as early as possible during
   * startup, to avoid entanglements with code that might save a getenv()
   * result pointer.
   */
  argv = save_ps_display_args(argc, argv);

/*
 * If supported on the current platform, set up a handler to be called if
 * the backend/postmaster crashes with a fatal signal or exception.
 */
#if defined(WIN32) && defined(HAVE_MINIDUMP_TYPE)
  pgwin32_install_crashdump_handler();
#endif

  /*
   * Fire up essential subsystems: error and memory management
   *
   * Code after this point is allowed to use elog/ereport, though
   * localization of messages may not work right away, and messages won't go
   * anywhere but stderr until GUC settings get loaded.
   */
  MemoryContextInit();

  /*
   * Set up locale information from environment.  Note that LC_CTYPE and
   * LC_COLLATE will be overridden later from pg_control if we are in an
   * already-initialized database.  We set them here so that they will be
   * available to fill pg_control during initdb.  LC_MESSAGES will get set
   * later during GUC option processing, but we set it here to allow startup
   * error messages to be localized.
   */

  set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("postgres"));

  init_locale(LC_COLLATE, "");
  init_locale(LC_CTYPE, "");

#ifdef LC_MESSAGES
  init_locale(LC_MESSAGES, "");
#endif

  /*
   * We keep these set to "C" always, except transiently in pg_locale.c; see
   * that file for explanations.
   */
  init_locale(LC_MONETARY, "C");
  init_locale(LC_NUMERIC, "C");
  init_locale(LC_TIME, "C");

  /*
   * Now that we have absorbed as much as we wish to from the locale
   * environment, remove any LC_ALL setting, so that the environment
   * variables installed by pg_perm_setlocale have force.
   */
  unsetenv("LC_ALL");

  /*
   * Catch standard options before doing much else, in particular before we
   * insist on not being root.
   */
  if (argc > 1) {
    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
      help(progname);
      exit(0);
    }
    if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
      puts("postgres (PostgreSQL) " PG_VERSION);
      exit(0);
    }

    /*
     * In addition to the above, we allow "--describe-config" and "-C var"
     * to be called by root.  This is reasonably safe since these are
     * read-only activities.  The -C case is important because pg_ctl may
     * try to invoke it while still holding administrator privileges on
     * Windows.  Note that while -C can normally be in any argv position,
     * if you wanna bypass the root check you gotta put it first.  This
     * reduces the risk that we might misinterpret some other mode's -C
     * switch as being the postmaster/postgres one.
     */
    if (strcmp(argv[1], "--describe-config") == 0)
      do_check_root = false;
    else if (argc > 2 && strcmp(argv[1], "-C") == 0)
      do_check_root = false;
  }

  /*
   * Make sure we are not running as root, unless it's safe for the selected
   * option.
   */
  if (do_check_root) check_root(progname);

/*
 * Dispatch to one of various subprograms depending on first argument.
 */

#ifdef EXEC_BACKEND
  if (argc > 1 && strncmp(argv[1], "--fork", 6) == 0)
    SubPostmasterMain(argc, argv); /* does not return */
#endif

  if (argc > 1 && strcmp(argv[1], "--boot") == 0)
    AuxiliaryProcessMain(argc, argv); /* does not return */
  else if (argc > 1 && strcmp(argv[1], "--describe-config") == 0)
    GucInfoMain(); /* does not return */
  else if (argc > 1 && strcmp(argv[1], "--single") == 0)
    PostgresMain(argc, argv, NULL,                         /* no dbname */
                 strdup(get_user_name_or_exit(progname))); /* does not return */
  else
    PostmasterMain(argc, argv); /* does not return */
  abort();                      /* should not get here */
}

/*
 * Place platform-specific startup hacks here.  This is the right
 * place to put code that must be executed early in the launch of any new
 * server process.  Note that this code will NOT be executed when a backend
 * or sub-bootstrap process is forked, unless we are in a fork/exec
 * environment (ie EXEC_BACKEND is defined).
 *
 * XXX The need for code here is proof that the platform in question
 * is too brain-dead to provide a standard C execution environment
 * without help.  Avoid adding more here, if you can.
 */
static void startup_hacks(const char *progname UNUSED_ATTRIBUTE) {
  /*
   * Initialize dummy_spinlock, in case we are on a platform where we have
   * to use the fallback implementation of pg_memory_barrier().
   */
  SpinLockInit(&dummy_spinlock);
}

/*
 * Make the initial permanent setting for a locale category.  If that fails,
 * perhaps due to LC_foo=invalid in the environment, use locale C.  If even
 * that fails, perhaps due to out-of-memory, the entire startup fails with it.
 * When this returns, we are guaranteed to have a setting for the given
 * category's environment variable.
 */
static void init_locale(int category, const char *locale) {
  if (pg_perm_setlocale(category, locale) == NULL &&
      pg_perm_setlocale(category, "C") == NULL)
    elog(FATAL, "could not adopt C locale");
}

/*
 * Help display should match the options accepted by PostmasterMain()
 * and PostgresMain().
 *
 * XXX On Windows, non-ASCII localizations of these messages only display
 * correctly if the console output code page covers the necessary characters.
 * Messages emitted in write_console() do not exhibit this problem.
 */
static void help(const char *progname) {
  fprintf(stderr,"%s is the PostgreSQL server.\n\n", progname);
  fprintf(stderr,"Usage:\n  %s [OPTION]...\n\n", progname);
  fprintf(stderr,"Options:\n");
  fprintf(stderr,"  -B NBUFFERS        number of shared buffers\n");
  fprintf(stderr,"  -c NAME=VALUE      set run-time parameter\n");
  fprintf(stderr,"  -C NAME            print value of run-time parameter, then exit\n");
  fprintf(stderr,"  -d 1-5             debugging level\n");
  fprintf(stderr,"  -D DATADIR         database directory\n");
  fprintf(stderr,"  -e                 use European date input format (DMY)\n");
  fprintf(stderr,"  -F                 turn fsync off\n");
  fprintf(stderr,"  -h HOSTNAME        host name or IP address to listen on\n");
  fprintf(stderr,"  -i                 enable TCP/IP connections\n");
  fprintf(stderr,"  -k DIRECTORY       Unix-domain socket location\n");
#ifdef USE_SSL
  fprintf(stderr,"  -l                 enable SSL connections\n");
#endif
  fprintf(stderr,"  -N MAX-CONNECT     maximum number of allowed connections\n");
  fprintf(stderr,"  -o OPTIONS         pass \"OPTIONS\" to each server process "
        "(obsolete)\n");
  fprintf(stderr,"  -p PORT            port number to listen on\n");
  fprintf(stderr,"  -s                 show statistics after each query\n");
  fprintf(stderr,"  -S WORK-MEM        set amount of memory for sorts (in kB)\n");
  fprintf(stderr,"  -V, --version      output version information, then exit\n");
  fprintf(stderr,"  --NAME=VALUE       set run-time parameter\n");
  fprintf(stderr,"  --describe-config  describe configuration parameters, then exit\n");
  fprintf(stderr,"  -?, --help         show this help, then exit\n");

  fprintf(stderr,"\nDeveloper options:\n");
  fprintf(stderr,"  -f s|i|n|m|h       forbid use of some plan types\n");
  fprintf(stderr,"  -n                 do not reinitialize shared memory after abnormal "
        "exit\n");
  fprintf(stderr,"  -O                 allow system table structure changes\n");
  fprintf(stderr,"  -P                 disable system indexes\n");
  fprintf(stderr,"  -t pa|pl|ex        show timings after each query\n");
  fprintf(stderr,"  -T                 send SIGSTOP to all backend processes if one "
        "dies\n");
  fprintf(stderr,"  -W NUM             wait NUM seconds to allow attach from a "
        "debugger\n");

  fprintf(stderr,"\nOptions for single-user mode:\n");
  fprintf(stderr,"  --single           selects single-user mode (must be first "
        "argument)\n");
  fprintf(stderr,"  DBNAME             database name (defaults to user name)\n");
  fprintf(stderr,"  -d 0-5             override debugging level\n");
  fprintf(stderr,"  -E                 echo statement before execution\n");
  fprintf(stderr,"  -j                 do not use newline as interactive query "
        "delimiter\n");
  fprintf(stderr,"  -r FILENAME        send stdout and stderr to given file\n");

  fprintf(stderr,"\nOptions for bootstrapping mode:\n");
  fprintf(stderr,"  --boot             selects bootstrapping mode (must be first "
        "argument)\n");
  fprintf(stderr,"  DBNAME             database name (mandatory argument in "
        "bootstrapping mode)\n");
  fprintf(stderr,"  -r FILENAME        send stdout and stderr to given file\n");
  fprintf(stderr,"  -x NUM             internal use\n");

  fprintf(stderr,"\nPlease read the documentation for the complete list of run-time\n"
        "configuration settings and how to set them on the command line or in\n"
        "the configuration file.\n\n"
        "Report bugs to <pgsql-bugs@postgresql.org>.\n");
}

static void check_root(const char *progname) {
  if (geteuid() == 0) {
    write_stderr(
        "\"root\" execution of the PostgreSQL server is not permitted.\n"
        "The server must be started under an unprivileged user ID to prevent\n"
        "possible system security compromise.  See the documentation for\n"
        "more information on how to properly start the server.\n");
    exit(1);
  }

  /*
   * Also make sure that real and effective uids are the same. Executing as
   * a setuid program from a root shell is a security hole, since on many
   * platforms a nefarious subroutine could setuid back to root if real uid
   * is root.  (Since nobody actually uses postgres as a setuid program,
   * trying to actively fix this situation seems more trouble than it's
   * worth; we'll just expend the effort to check for it.)
   */
  if (getuid() != geteuid()) {
    write_stderr("%s: real and effective user IDs must match\n", progname);
    exit(1);
  }
}
