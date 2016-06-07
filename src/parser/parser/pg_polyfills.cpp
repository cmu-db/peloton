/* Polyfills to avoid building unnecessary objects from the PostgreSQL source */

#include "parser/postgres.h"

/* src/backend/postmaster/postmaster.c */
// TODO: Peloton Changes
thread_local bool ClientAuthInProgress = false;
thread_local bool redirection_done = false;

/* src/backend/postmaster/syslogger.c */
bool am_syslogger = false;

/* src/backend/tcop/postgres.c */
#include "parser/tcop/dest.h"
thread_local const char *debug_query_string;
thread_local CommandDest whereToSendOutput = DestDebug;

/* src/backend/utils/misc/guc.c */
char *application_name;
int client_min_messages = NOTICE;
int log_min_error_statement = ERROR;
int log_min_messages = WARNING;
int trace_recovery_messages = LOG;
bool assert_enabled = true;

/* src/backend/storage/lmgr/proc.c */
#include "parser/storage/proc.h"
// TODO: Peloton changes
thread_local PGPROC *MyProc = NULL;

/* src/backend/storage/ipc/ipc.c */
// TODO: Peloton changes
thread_local bool proc_exit_inprogress = false;

/* src/backend/tcop/postgres.c */
#include "parser/miscadmin.h"
void check_stack_depth(void) { /* Do nothing */ }

/* src/backends/commands/define.c */
#include "parser/commands/defrem.h"
#include "parser/nodes/makefuncs.h"
DefElem * defWithOids(bool value)
{
  return makeDefElem("oids", (Node *) makeInteger(value));
}

/* src/timezone/pgtz.c */
pg_tz *log_timezone = NULL;

/* We don't even need these below, but the linker otherwise complains about them being used in files where we need some things */

/* "_DirectFunctionCall1Coll", referenced from:
      _pg_convert_to in libpg_query.a(mbutils.o)
      _pg_convert_from in libpg_query.a(mbutils.o)
      _getdatabaseencoding in libpg_query.a(mbutils.o)
      _pg_client_encoding in libpg_query.a(mbutils.o)
      _current_user in libpg_query.a(name.o)
      _session_user in libpg_query.a(name.o)
      _current_schema in libpg_query.a(name.o)
      ... */
Datum DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1)
{
  Datum dummy = {0};
  return dummy;
}

/* "_DirectFunctionCall3Coll", referenced from:
      _pg_convert_to in libpg_query.a(mbutils.o)
      _pg_convert_from in libpg_query.a(mbutils.o) */
Datum DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3)
{
  Datum dummy = {0};
  return dummy;
}

/* "_FunctionCall5Coll", referenced from:
      _perform_default_encoding_conversion in libpg_query.a(mbutils.o) */
Datum FunctionCall5Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5){
  Datum dummy = {0};
  return dummy;
}

/* "_OidFunctionCall5Coll", referenced from:
      _pg_do_encoding_conversion in libpg_query.a(mbutils.o) */
Datum OidFunctionCall5Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
  Datum dummy = {0};
  return dummy;
}

/* "_FindDefaultConversionProc", referenced from:
      _PrepareClientEncoding in libpg_query.a(mbutils.o)
      _pg_do_encoding_conversion in libpg_query.a(mbutils.o) */
#include "parser/catalog/namespace.h"
Oid FindDefaultConversionProc(int32 for_encoding, int32 to_encoding)
{
  return InvalidOid;
}

/* "_GetSessionUserId", referenced from:
      _session_user in libpg_query.a(name.o) */
Oid GetSessionUserId(void)
{
  return 0;
}

/* "_GetUserId", referenced from:
      _current_user in libpg_query.a(name.o) */
Oid GetUserId(void)
{
  return 0;
}

/* "_GetUserNameFromId", referenced from:
      _current_user in libpg_query.a(name.o)
      _session_user in libpg_query.a(name.o) */
char * GetUserNameFromId(Oid roleid, bool noerr)
{
  return "dummy";
}

/* "_GetTopTransactionIdIfAny", referenced from:
      _log_line_prefix in libpg_query.a(elog.o)
      _write_csvlog in libpg_query.a(elog.o) */
#include "parser/access/xact.h"
TransactionId GetTopTransactionIdIfAny(void)
{
  TransactionId dummy = {0};
  return dummy;
}

/* "_IsTransactionState", referenced from:
      _PrepareClientEncoding in libpg_query.a(mbutils.o)
      _pg_do_encoding_conversion in libpg_query.a(mbutils.o) */
#include "parser/access/xact.h"
bool IsTransactionState(void)
{
  return false;
}

/* "_NameListToString", referenced from:
      _base_yyparse in libpg_query.a(gram.o)
      _makeRangeVarFromAnyName in libpg_query.a(gram.o) */
char * NameListToString(List *names)
{
  return "dummy";
}

/* "_ProcessInterrupts", referenced from:
      _errfinish in libpg_query.a(elog.o) */
void ProcessInterrupts(void) {
  /* do nothing */
}

/* "_construct_array", referenced from:
      _current_schemas in libpg_query.a(name.o) */
ArrayType * construct_array(Datum *elems, int nelems, Oid elmtype, int elmlen, bool elmbyval, char elmalign)
{
  return NULL;
}

/* "_fetch_search_path", referenced from:
      _current_schema in libpg_query.a(name.o)
      _current_schemas in libpg_query.a(name.o) */
List * fetch_search_path(bool includeImplicit)
{
  return NULL;
}

/* "_fmgr_info_cxt", referenced from:
      _PrepareClientEncoding in libpg_query.a(mbutils.o) */
void fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt)
{
  /* do nothing */
}

/*  "_format_type_be", referenced from:
      _exprType in libpg_query.a(nodeFuncs.o) */
#include "parser/utils/builtins.h"
char * format_type_be(Oid type_oid) {
  return NULL;
}

/* "_get_array_type", referenced from:
      _exprType in libpg_query.a(nodeFuncs.o) */
#include "parser/utils/lsyscache.h"
Oid get_array_type(Oid typid)
{
  return 0;
}

/* "_get_namespace_name", referenced from:
      _current_schema in libpg_query.a(name.o)
      _current_schemas in libpg_query.a(name.o) */
#include "parser/utils/lsyscache.h"
char * get_namespace_name(Oid nspid)
{
  return "dummy";
}

/*  "_get_promoted_array_type", referenced from:
      _exprType in libpg_query.a(nodeFuncs.o) */
#include "parser/utils/lsyscache.h"
Oid get_promoted_array_type(Oid typid)
{
  return InvalidOid;
}

/* "_get_ps_display", referenced from:
      _log_line_prefix in libpg_query.a(elog.o)
      _write_csvlog in libpg_query.a(elog.o) */
#include "parser/utils/ps_status.h"
const char * get_ps_display(int *displen)
{
  return "dummy";
}

/* "_get_rel_type_id", referenced from:
      _makeWholeRowVar in libpg_query.a(makefuncs.o) */
#include "parser/utils/lsyscache.h"
Oid get_rel_type_id(Oid relid) {
  return 0;
}

/*  "_get_typlenbyval", referenced from:
      _makeNullConst in libpg_query.a(makefuncs.o) */
#include "parser/utils/lsyscache.h"
void get_typlenbyval(Oid typid, int16 *typlen, bool *typbyval)
{
  /* do nothing */
}

/* "_hash_any", referenced from:
      _bms_hash_value in libpg_query.a(bitmapset.o) */
#include "parser/access/hash.h"
Datum hash_any(register const unsigned char *k, register int keylen)
{
  Datum dummy = {0};
  return dummy;
}

/*   "_pg_detoast_datum", referenced from:
      _makeConst in libpg_query.a(makefuncs.o) */
struct varlena * pg_detoast_datum(struct varlena * datum) {
  return NULL;
}

/* "_pg_detoast_datum_packed", referenced from:
      _pg_convert in libpg_query.a(mbutils.o)
      _length_in_encoding in libpg_query.a(mbutils.o) */
struct varlena * pg_detoast_datum_packed(struct varlena * datum) {
  return NULL;
}

/* "_pg_localtime", referenced from:
      _log_line_prefix in libpg_query.a(elog.o)
      _setup_formatted_log_time in libpg_query.a(elog.o)
      _setup_formatted_start_time in libpg_query.a(elog.o) */
struct pg_tm * pg_localtime(const pg_time_t *timep, const pg_tz *tz)
{
  return NULL;
}

/* "_pg_strftime", referenced from:
      _log_line_prefix in libpg_query.a(elog.o)
      _setup_formatted_log_time in libpg_query.a(elog.o)
      _setup_formatted_start_time in libpg_query.a(elog.o) */
size_t pg_strftime(char *s, size_t maxsize, const char *format, const struct pg_tm * t)
{
  return 0;
}

/* "_pg_wchar_strlen", referenced from:
      _pg_wchar2mb in libpg_query.a(mbutils.o) */
#include "parser/mb/pg_wchar.h"
size_t pg_wchar_strlen(const pg_wchar *str)
{
  return 0;
}

/* "_proc_exit", referenced from:
      _errfinish in libpg_query.a(elog.o) */
#include "parser/storage/ipc.h"
void proc_exit(int code)
{
  exit(1);
}

/* "_type_is_rowtype", referenced from:
      _makeWholeRowVar in libpg_query.a(makefuncs.o) */
#include "parser/utils/lsyscache.h"
bool type_is_rowtype(Oid typid) {
  return false;
}

/* "_write_syslogger_file", referenced from:
      _send_message_to_server_log in libpg_query.a(elog.o)
      _write_csvlog in libpg_query.a(elog.o) */
#include "parser/postmaster/syslogger.h"
void write_syslogger_file(const char *buffer, int count, int destination) {
  /* do nothing */
}

/* "_PqCommMethods", referenced from:
      _errfinish in libpg_query.a(elog.o)
      _send_message_to_frontend in libpg_query.a(elog.o)
      _pq_endmessage in libpg_query.a(pqformat.o)
      _pq_puttextmessage in libpg_query.a(pqformat.o)
      _pq_putemptymessage in libpg_query.a(pqformat.o) */
#include "parser/libpq/libpq.h"
PQcommMethods *PqCommMethods = NULL;

/* "_operator_precedence_warning", referenced from:
      _base_yyparse in libpg_query.a(gram.o) */
bool operator_precedence_warning = false;
