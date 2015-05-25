/*-------------------------------------------------------------------------
 *
 * outfuncs.c
 *	  Output functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/outfuncs.c
 *
 * NOTES
 *	  Every node type that can appear in stored rules' parsetrees *must*
 *	  have an output function defined here (as well as an input function
 *	  in readfuncs.c).  For use in debugging, we also provide output
 *	  functions for nodes that appear in raw parsetrees, path, and plan trees.
 *	  These nodes however need not have input functions.
 *
 *-------------------------------------------------------------------------
 */

#include <string>
#include <sstream>
#include <ctype.h>

#include "backend/outfuncs.h"
#include "common/logger.h"

#include "postgres.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/datum.h"
#include "utils/expandeddatum.h"

namespace nstore {
namespace backend {

void appendStringInfo(std::stringstream& str, const char *fmt, ...){

  int n, size=100;
  bool b=false;
  va_list   args;
  std::string s;

  /* Try to format the data. */
  while (!b) {
    s.resize(size);

    va_start(args, fmt);
    n = vsnprintf((char*)s.c_str(), size, fmt, args);
    va_end(args);

    if ((n>0) && ((b=(n<size))==true))
      s.resize(n);
    else
      size*=2;
  }

  // append string to stream
  str << s << "\n";
}

void appendStringInfoString(std::stringstream& str, const char *s){
  str << s;
}

void appendStringInfoChar(std::stringstream& str, char ch){
  str << ch;
}


/*-------------------------------------------------------------------------
 * datumGetSize
 *
 * Find the "real" size of a datum, given the datum value,
 * whether it is a "by value", and the declared type length.
 * (For TOAST pointer datums, this is the size of the pointer datum.)
 *
 * This is essentially an out-of-line version of the att_addlength_datum()
 * macro in access/tupmacs.h.  We do a tad more error checking though.
 *-------------------------------------------------------------------------
 */
Size
datumGetSize(Datum value, bool typByVal, int typLen)
{
  Size    size;

  if (typByVal)
  {
    /* Pass-by-value types are always fixed-length */
    Assert(typLen > 0 && typLen <= sizeof(Datum));
    size = (Size) typLen;
  }
  else
  {
    if (typLen > 0)
    {
      /* Fixed-length pass-by-ref type */
      size = (Size) typLen;
    }
    else if (typLen == -1)
    {
      /* It is a varlena datatype */
      struct varlena *s = (struct varlena *) DatumGetPointer(value);

      if (!PointerIsValid(s))
        LOG_ERROR("invalid Datum pointer");

      size = (Size) VARSIZE_ANY(s);
    }
    else if (typLen == -2)
    {
      /* It is a cstring datatype */
      char     *s = (char *) DatumGetPointer(value);

      if (!PointerIsValid(s))
        LOG_ERROR("invalid Datum pointer");

      size = (Size) (strlen(s) + 1);
    }
    else
    {
      LOG_ERROR("invalid typLen: %d", typLen);

      size = 0;     /* keep compiler quiet */
    }
  }

  return size;
}

/*
 * Macros to simplify output of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/* Write the label for the node type */
#define WRITE_NODE_TYPE(nodelabel) \
    appendStringInfoString(str, nodelabel)

/* Write an integer field (anything written as ":fldname %d") */
#define WRITE_INT_FIELD(fldname) \
    appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write an unsigned integer field (anything written as ":fldname %u") */
#define WRITE_UINT_FIELD(fldname) \
    appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
    appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
    appendStringInfo(str, " :" CppAsString(fldname) " %ld", node->fldname)

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
    appendStringInfo(str, " :" CppAsString(fldname) " %c", node->fldname)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
    appendStringInfo(str, " :" CppAsString(fldname) " %d", \
                     (int) node->fldname)

/* Write a float field --- caller must give format to define precision */
#define WRITE_FLOAT_FIELD(fldname,format) \
    appendStringInfo(str, " :" CppAsString(fldname) " " format, node->fldname)

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
    appendStringInfo(str, " :" CppAsString(fldname) " %s", \
                     booltostr(node->fldname))

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), \
        _outToken(str, node->fldname))

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
    appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), \
        _outNode(str, node->fldname))

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), \
        _outBitmapset(str, node->fldname))


#define booltostr(x)  ((x) ? "true" : "false")

static void _outNode(std::stringstream& str, const void *obj);


/*
 * _outToken
 *	  Convert an ordinary string (eg, an identifier) into a form that
 *	  will be decoded back to a plain token by read.c's functions.
 *
 *	  If a null or empty string is given, it is encoded as "<>".
 */
static void
_outToken(std::stringstream& str, const char *s)
{
  if (s == NULL || *s == '\0')
  {
    appendStringInfoString(str, "<>");
    return;
  }

  /*
   * Look for characters or patterns that are treated specially by read.c
   * (either in pg_strtok() or in nodeRead()), and therefore need a
   * protective backslash.
   */
  /* These characters only need to be quoted at the start of the string */
  if (*s == '<' ||
      *s == '\"' ||
      isdigit((unsigned char) *s) ||
      ((*s == '+' || *s == '-') &&
          (isdigit((unsigned char) s[1]) || s[1] == '.')))
    appendStringInfoChar(str, '\\');
  while (*s)
  {
    /* These chars must be backslashed anywhere in the string */
    if (*s == ' ' || *s == '\n' || *s == '\t' ||
        *s == '(' || *s == ')' || *s == '{' || *s == '}' ||
        *s == '\\')
      appendStringInfoChar(str, '\\');
    appendStringInfoChar(str, *s++);
  }
}

static void
_outList(std::stringstream& str, const List *node)
{
  const ListCell *lc;

  appendStringInfoChar(str, '(');

  if (IsA(node, IntList))
    appendStringInfoChar(str, 'i');
  else if (IsA(node, OidList))
    appendStringInfoChar(str, 'o');

  foreach(lc, node)
  {
    /*
     * For the sake of backward compatibility, we emit a slightly
     * different whitespace format for lists of nodes vs. other types of
     * lists. XXX: is this necessary?
     */
    if (IsA(node, List))
    {
      _outNode(str, lfirst(lc));
      if (lnext(lc))
        appendStringInfoChar(str, ' ');
    }
    else if (IsA(node, IntList))
      appendStringInfo(str, " %d", lfirst_int(lc));
    else if (IsA(node, OidList))
      appendStringInfo(str, " %u", lfirst_oid(lc));
    /*
    else
      LOG_ERROR("unrecognized list node type: %d",
                (int) node->type);
    */
  }

  appendStringInfoChar(str, ')');
}

/*
 * _outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Note: the output format is "(b int int ...)", similar to an integer List.
 */
static void
_outBitmapset(std::stringstream& str, const Bitmapset *bms __attribute__((unused)))
{
  int			x;

  appendStringInfoChar(str, '(');
  appendStringInfoChar(str, 'b');
  x = -1;
  // TODO: Disable for now
  //while ((x = bms_next_member(bms, x)) >= 0)
  appendStringInfo(str, " %d", x);

  appendStringInfoChar(str, ')');

}


/*
 * Print the value of a Datum given its type.
 */
static void
_outDatum(std::stringstream& str, Datum value, int typlen, bool typbyval)
{
  Size		length,
  i;
  char	   *s;

  length = datumGetSize(value, typbyval, typlen);

  if (typbyval)
  {
    s = (char *) (&value);
    appendStringInfo(str, "%u [ ", (unsigned int) length);
    for (i = 0; i < (Size) sizeof(Datum); i++)
      appendStringInfo(str, "%d ", (int) (s[i]));
    appendStringInfoChar(str, ']');
  }
  else
  {
    s = (char *) DatumGetPointer(value);
    if (!PointerIsValid(s))
      appendStringInfoString(str, "0 [ ]");
    else
    {
      appendStringInfo(str, "%u [ ", (unsigned int) length);
      for (i = 0; i < length; i++)
        appendStringInfo(str, "%d ", (int) (s[i]));
      appendStringInfoChar(str, ']');
    }
  }
}


/*
 *	Stuff from plannodes.h
 */

static void
_outPlannedStmt(std::stringstream& str, const PlannedStmt *node)
{
  WRITE_NODE_TYPE("PLANNEDSTMT");

  WRITE_ENUM_FIELD(commandType, CmdType);
  WRITE_UINT_FIELD(queryId);
  WRITE_BOOL_FIELD(hasReturning);
  WRITE_BOOL_FIELD(hasModifyingCTE);
  WRITE_BOOL_FIELD(canSetTag);
  WRITE_BOOL_FIELD(transientPlan);
  WRITE_NODE_FIELD(planTree);
  WRITE_NODE_FIELD(rtable);
  WRITE_NODE_FIELD(resultRelations);
  WRITE_NODE_FIELD(utilityStmt);
  WRITE_NODE_FIELD(subplans);
  WRITE_BITMAPSET_FIELD(rewindPlanIDs);
  WRITE_NODE_FIELD(rowMarks);
  WRITE_NODE_FIELD(relationOids);
  WRITE_NODE_FIELD(invalItems);
  WRITE_INT_FIELD(nParamExec);
  WRITE_BOOL_FIELD(hasRowSecurity);
}

/*
 * print the basic stuff of all nodes that inherit from Plan
 */
static void
_outPlanInfo(std::stringstream& str, const Plan *node)
{
  WRITE_FLOAT_FIELD(startup_cost, "%.2f");
  WRITE_FLOAT_FIELD(total_cost, "%.2f");
  WRITE_FLOAT_FIELD(plan_rows, "%.0f");
  WRITE_INT_FIELD(plan_width);
  WRITE_NODE_FIELD(targetlist);
  WRITE_NODE_FIELD(qual);
  WRITE_NODE_FIELD(lefttree);
  WRITE_NODE_FIELD(righttree);
  WRITE_NODE_FIELD(initPlan);
  WRITE_BITMAPSET_FIELD(extParam);
  WRITE_BITMAPSET_FIELD(allParam);
}

/*
 * print the basic stuff of all nodes that inherit from Scan
 */
static void
_outScanInfo(std::stringstream& str, const Scan *node)
{
  _outPlanInfo(str, (const Plan *) node);

  WRITE_UINT_FIELD(scanrelid);
}

/*
 * print the basic stuff of all nodes that inherit from Join
 */
static void
_outJoinPlanInfo(std::stringstream& str, const Join *node)
{
  _outPlanInfo(str, (const Plan *) node);

  WRITE_ENUM_FIELD(jointype, JoinType);
  WRITE_NODE_FIELD(joinqual);
}


static void
_outPlan(std::stringstream& str, const Plan *node)
{
  WRITE_NODE_TYPE("PLAN");

  _outPlanInfo(str, (const Plan *) node);
}

static void
_outResult(std::stringstream& str, const Result *node)
{
  WRITE_NODE_TYPE("RESULT");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_NODE_FIELD(resconstantqual);
}

static void
_outModifyTable(std::stringstream& str, const ModifyTable *node)
{
  WRITE_NODE_TYPE("MODIFYTABLE");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_ENUM_FIELD(operation, CmdType);
  WRITE_BOOL_FIELD(canSetTag);
  WRITE_UINT_FIELD(nominalRelation);
  WRITE_NODE_FIELD(resultRelations);
  WRITE_INT_FIELD(resultRelIndex);
  WRITE_NODE_FIELD(plans);
  WRITE_NODE_FIELD(withCheckOptionLists);
  WRITE_NODE_FIELD(returningLists);
  WRITE_NODE_FIELD(fdwPrivLists);
  WRITE_NODE_FIELD(rowMarks);
  WRITE_INT_FIELD(epqParam);
  WRITE_ENUM_FIELD(onConflictAction, OnConflictAction);
  WRITE_NODE_FIELD(arbiterIndexes);
  WRITE_NODE_FIELD(onConflictSet);
  WRITE_NODE_FIELD(onConflictWhere);
  WRITE_INT_FIELD(exclRelRTI);
  WRITE_NODE_FIELD(exclRelTlist);
}

static void
_outAppend(std::stringstream& str, const Append *node)
{
  WRITE_NODE_TYPE("APPEND");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_NODE_FIELD(appendplans);
}

static void
_outMergeAppend(std::stringstream& str, const MergeAppend *node)
{
  int			i;

  WRITE_NODE_TYPE("MERGEAPPEND");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_NODE_FIELD(mergeplans);

  WRITE_INT_FIELD(numCols);

  appendStringInfoString(str, " :sortColIdx");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %d", node->sortColIdx[i]);

  appendStringInfoString(str, " :sortOperators");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->sortOperators[i]);

  appendStringInfoString(str, " :collations");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->collations[i]);

  appendStringInfoString(str, " :nullsFirst");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %s", booltostr(node->nullsFirst[i]));
}

static void
_outRecursiveUnion(std::stringstream& str, const RecursiveUnion *node)
{
  int			i;

  WRITE_NODE_TYPE("RECURSIVEUNION");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_INT_FIELD(wtParam);
  WRITE_INT_FIELD(numCols);

  appendStringInfoString(str, " :dupColIdx");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %d", node->dupColIdx[i]);

  appendStringInfoString(str, " :dupOperators");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->dupOperators[i]);

  WRITE_LONG_FIELD(numGroups);
}

static void
_outBitmapAnd(std::stringstream& str, const BitmapAnd *node)
{
  WRITE_NODE_TYPE("BITMAPAND");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_NODE_FIELD(bitmapplans);
}

static void
_outBitmapOr(std::stringstream& str, const BitmapOr *node)
{
  WRITE_NODE_TYPE("BITMAPOR");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_NODE_FIELD(bitmapplans);
}

static void
_outScan(std::stringstream& str, const Scan *node)
{
  WRITE_NODE_TYPE("SCAN");

  _outScanInfo(str, node);
}

static void
_outSeqScan(std::stringstream& str, const SeqScan *node)
{
  WRITE_NODE_TYPE("SEQSCAN");

  _outScanInfo(str, (const Scan *) node);
}

static void
_outIndexScan(std::stringstream& str, const IndexScan *node)
{
  WRITE_NODE_TYPE("INDEXSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_OID_FIELD(indexid);
  WRITE_NODE_FIELD(indexqual);
  WRITE_NODE_FIELD(indexqualorig);
  WRITE_NODE_FIELD(indexorderby);
  WRITE_NODE_FIELD(indexorderbyorig);
  WRITE_NODE_FIELD(indexorderbyops);
  WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void
_outIndexOnlyScan(std::stringstream& str, const IndexOnlyScan *node)
{
  WRITE_NODE_TYPE("INDEXONLYSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_OID_FIELD(indexid);
  WRITE_NODE_FIELD(indexqual);
  WRITE_NODE_FIELD(indexorderby);
  WRITE_NODE_FIELD(indextlist);
  WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void
_outBitmapIndexScan(std::stringstream& str, const BitmapIndexScan *node)
{
  WRITE_NODE_TYPE("BITMAPINDEXSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_OID_FIELD(indexid);
  WRITE_NODE_FIELD(indexqual);
  WRITE_NODE_FIELD(indexqualorig);
}

static void
_outBitmapHeapScan(std::stringstream& str, const BitmapHeapScan *node)
{
  WRITE_NODE_TYPE("BITMAPHEAPSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_NODE_FIELD(bitmapqualorig);
}

static void
_outTidScan(std::stringstream& str, const TidScan *node)
{
  WRITE_NODE_TYPE("TIDSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_NODE_FIELD(tidquals);
}

static void
_outSubqueryScan(std::stringstream& str, const SubqueryScan *node)
{
  WRITE_NODE_TYPE("SUBQUERYSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_NODE_FIELD(subplan);
}

static void
_outFunctionScan(std::stringstream& str, const FunctionScan *node)
{
  WRITE_NODE_TYPE("FUNCTIONSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_NODE_FIELD(functions);
  WRITE_BOOL_FIELD(funcordinality);
}

static void
_outValuesScan(std::stringstream& str, const ValuesScan *node)
{
  WRITE_NODE_TYPE("VALUESSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_NODE_FIELD(values_lists);
}

static void
_outCteScan(std::stringstream& str, const CteScan *node)
{
  WRITE_NODE_TYPE("CTESCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_INT_FIELD(ctePlanId);
  WRITE_INT_FIELD(cteParam);
}

static void
_outWorkTableScan(std::stringstream& str, const WorkTableScan *node)
{
  WRITE_NODE_TYPE("WORKTABLESCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_INT_FIELD(wtParam);
}

static void
_outForeignScan(std::stringstream& str, const ForeignScan *node)
{
  WRITE_NODE_TYPE("FOREIGNSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_OID_FIELD(fs_server);
  WRITE_NODE_FIELD(fdw_exprs);
  WRITE_NODE_FIELD(fdw_private);
  WRITE_NODE_FIELD(fdw_scan_tlist);
  WRITE_BITMAPSET_FIELD(fs_relids);
  WRITE_BOOL_FIELD(fsSystemCol);
}

static void
_outCustomScan(std::stringstream& str, const CustomScan *node)
{
  WRITE_NODE_TYPE("CUSTOMSCAN");

  _outScanInfo(str, (const Scan *) node);

  WRITE_UINT_FIELD(flags);
  WRITE_NODE_FIELD(custom_exprs);
  WRITE_NODE_FIELD(custom_private);
  WRITE_NODE_FIELD(custom_scan_tlist);
  WRITE_BITMAPSET_FIELD(custom_relids);
  appendStringInfoString(str, " :methods ");
  _outToken(str, node->methods->CustomName);
  // TODO: Fix conversion to StringInfo
  //if (node->methods->TextOutCustomScan)
  //	node->methods->TextOutCustomScan(str, node);
}

static void
_outSampleScan(std::stringstream& str, const SampleScan *node)
{
  WRITE_NODE_TYPE("SAMPLESCAN");

  _outScanInfo(str, (const Scan *) node);
}

static void
_outJoin(std::stringstream& str, const Join *node)
{
  WRITE_NODE_TYPE("JOIN");

  _outJoinPlanInfo(str, (const Join *) node);
}

static void
_outNestLoop(std::stringstream& str, const NestLoop *node)
{
  WRITE_NODE_TYPE("NESTLOOP");

  _outJoinPlanInfo(str, (const Join *) node);

  WRITE_NODE_FIELD(nestParams);
}

static void
_outMergeJoin(std::stringstream& str, const MergeJoin *node)
{
  int			numCols;
  int			i;

  WRITE_NODE_TYPE("MERGEJOIN");

  _outJoinPlanInfo(str, (const Join *) node);

  WRITE_NODE_FIELD(mergeclauses);

  numCols = list_length(node->mergeclauses);

  appendStringInfoString(str, " :mergeFamilies");
  for (i = 0; i < numCols; i++)
    appendStringInfo(str, " %u", node->mergeFamilies[i]);

  appendStringInfoString(str, " :mergeCollations");
  for (i = 0; i < numCols; i++)
    appendStringInfo(str, " %u", node->mergeCollations[i]);

  appendStringInfoString(str, " :mergeStrategies");
  for (i = 0; i < numCols; i++)
    appendStringInfo(str, " %d", node->mergeStrategies[i]);

  appendStringInfoString(str, " :mergeNullsFirst");
  for (i = 0; i < numCols; i++)
    appendStringInfo(str, " %d", (int) node->mergeNullsFirst[i]);
}

static void
_outHashJoin(std::stringstream& str, const HashJoin *node)
{
  WRITE_NODE_TYPE("HASHJOIN");

  _outJoinPlanInfo(str, (const Join *) node);

  WRITE_NODE_FIELD(hashclauses);
}

static void
_outAgg(std::stringstream& str, const Agg *node)
{
  int			i;

  WRITE_NODE_TYPE("AGG");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
  WRITE_INT_FIELD(numCols);

  appendStringInfoString(str, " :grpColIdx");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %d", node->grpColIdx[i]);

  appendStringInfoString(str, " :grpOperators");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->grpOperators[i]);

  WRITE_LONG_FIELD(numGroups);

  WRITE_NODE_FIELD(groupingSets);
  WRITE_NODE_FIELD(chain);
}

static void
_outWindowAgg(std::stringstream& str, const WindowAgg *node)
{
  int			i;

  WRITE_NODE_TYPE("WINDOWAGG");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_UINT_FIELD(winref);
  WRITE_INT_FIELD(partNumCols);

  appendStringInfoString(str, " :partColIdx");
  for (i = 0; i < node->partNumCols; i++)
    appendStringInfo(str, " %d", node->partColIdx[i]);

  appendStringInfoString(str, " :partOperations");
  for (i = 0; i < node->partNumCols; i++)
    appendStringInfo(str, " %u", node->partOperators[i]);

  WRITE_INT_FIELD(ordNumCols);

  appendStringInfoString(str, " :ordColIdx");
  for (i = 0; i < node->ordNumCols; i++)
    appendStringInfo(str, " %d", node->ordColIdx[i]);

  appendStringInfoString(str, " :ordOperations");
  for (i = 0; i < node->ordNumCols; i++)
    appendStringInfo(str, " %u", node->ordOperators[i]);

  WRITE_INT_FIELD(frameOptions);
  WRITE_NODE_FIELD(startOffset);
  WRITE_NODE_FIELD(endOffset);
}

static void
_outGroup(std::stringstream& str, const Group *node)
{
  int			i;

  WRITE_NODE_TYPE("GROUP");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_INT_FIELD(numCols);

  appendStringInfoString(str, " :grpColIdx");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %d", node->grpColIdx[i]);

  appendStringInfoString(str, " :grpOperators");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->grpOperators[i]);
}

static void
_outMaterial(std::stringstream& str, const Material *node)
{
  WRITE_NODE_TYPE("MATERIAL");

  _outPlanInfo(str, (const Plan *) node);
}

static void
_outSort(std::stringstream& str, const Sort *node)
{
  int			i;

  WRITE_NODE_TYPE("SORT");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_INT_FIELD(numCols);

  appendStringInfoString(str, " :sortColIdx");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %d", node->sortColIdx[i]);

  appendStringInfoString(str, " :sortOperators");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->sortOperators[i]);

  appendStringInfoString(str, " :collations");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->collations[i]);

  appendStringInfoString(str, " :nullsFirst");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %s", booltostr(node->nullsFirst[i]));
}

static void
_outUnique(std::stringstream& str, const Unique *node)
{
  int			i;

  WRITE_NODE_TYPE("UNIQUE");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_INT_FIELD(numCols);

  appendStringInfoString(str, " :uniqColIdx");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %d", node->uniqColIdx[i]);

  appendStringInfoString(str, " :uniqOperators");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->uniqOperators[i]);
}

static void
_outHash(std::stringstream& str, const Hash *node)
{
  WRITE_NODE_TYPE("HASH");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_OID_FIELD(skewTable);
  WRITE_INT_FIELD(skewColumn);
  WRITE_BOOL_FIELD(skewInherit);
  WRITE_OID_FIELD(skewColType);
  WRITE_INT_FIELD(skewColTypmod);
}

static void
_outSetOp(std::stringstream& str, const SetOp *node)
{
  int			i;

  WRITE_NODE_TYPE("SETOP");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_ENUM_FIELD(cmd, SetOpCmd);
  WRITE_ENUM_FIELD(strategy, SetOpStrategy);
  WRITE_INT_FIELD(numCols);

  appendStringInfoString(str, " :dupColIdx");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %d", node->dupColIdx[i]);

  appendStringInfoString(str, " :dupOperators");
  for (i = 0; i < node->numCols; i++)
    appendStringInfo(str, " %u", node->dupOperators[i]);

  WRITE_INT_FIELD(flagColIdx);
  WRITE_INT_FIELD(firstFlag);
  WRITE_LONG_FIELD(numGroups);
}

static void
_outLockRows(std::stringstream& str, const LockRows *node)
{
  WRITE_NODE_TYPE("LOCKROWS");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_NODE_FIELD(rowMarks);
  WRITE_INT_FIELD(epqParam);
}

static void
_outLimit(std::stringstream& str, const Limit *node)
{
  WRITE_NODE_TYPE("LIMIT");

  _outPlanInfo(str, (const Plan *) node);

  WRITE_NODE_FIELD(limitOffset);
  WRITE_NODE_FIELD(limitCount);
}

static void
_outNestLoopParam(std::stringstream& str, const NestLoopParam *node)
{
  WRITE_NODE_TYPE("NESTLOOPPARAM");

  WRITE_INT_FIELD(paramno);
  WRITE_NODE_FIELD(paramval);
}

static void
_outPlanRowMark(std::stringstream& str, const PlanRowMark *node)
{
  WRITE_NODE_TYPE("PLANROWMARK");

  WRITE_UINT_FIELD(rti);
  WRITE_UINT_FIELD(prti);
  WRITE_UINT_FIELD(rowmarkId);
  WRITE_ENUM_FIELD(markType, RowMarkType);
  WRITE_INT_FIELD(allMarkTypes);
  WRITE_ENUM_FIELD(strength, LockClauseStrength);
  WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
  WRITE_BOOL_FIELD(isParent);
}

static void
_outPlanInvalItem(std::stringstream& str, const PlanInvalItem *node)
{
  WRITE_NODE_TYPE("PLANINVALITEM");

  WRITE_INT_FIELD(cacheId);
  WRITE_UINT_FIELD(hashValue);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void
_outAlias(std::stringstream& str, const Alias *node)
{
  WRITE_NODE_TYPE("ALIAS");

  WRITE_STRING_FIELD(aliasname);
  WRITE_NODE_FIELD(colnames);
}

static void
_outRangeVar(std::stringstream& str, const RangeVar *node)
{
  WRITE_NODE_TYPE("RANGEVAR");

  /*
   * we deliberately ignore catalogname here, since it is presently not
   * semantically meaningful
   */
  WRITE_STRING_FIELD(schemaname);
  WRITE_STRING_FIELD(relname);
  WRITE_ENUM_FIELD(inhOpt, InhOption);
  WRITE_CHAR_FIELD(relpersistence);
  WRITE_NODE_FIELD(alias);
  WRITE_LOCATION_FIELD(location);
}

static void
_outIntoClause(std::stringstream& str, const IntoClause *node)
{
  WRITE_NODE_TYPE("INTOCLAUSE");

  WRITE_NODE_FIELD(rel);
  WRITE_NODE_FIELD(colNames);
  WRITE_NODE_FIELD(options);
  WRITE_ENUM_FIELD(onCommit, OnCommitAction);
  WRITE_STRING_FIELD(tableSpaceName);
  WRITE_NODE_FIELD(viewQuery);
  WRITE_BOOL_FIELD(skipData);
}

static void
_outVar(std::stringstream& str, const Var *node)
{
  WRITE_NODE_TYPE("VAR");

  WRITE_UINT_FIELD(varno);
  WRITE_INT_FIELD(varattno);
  WRITE_OID_FIELD(vartype);
  WRITE_INT_FIELD(vartypmod);
  WRITE_OID_FIELD(varcollid);
  WRITE_UINT_FIELD(varlevelsup);
  WRITE_UINT_FIELD(varnoold);
  WRITE_INT_FIELD(varoattno);
  WRITE_LOCATION_FIELD(location);
}

static void
_outConst(std::stringstream& str, const Const *node)
{
  WRITE_NODE_TYPE("CONST");

  WRITE_OID_FIELD(consttype);
  WRITE_INT_FIELD(consttypmod);
  WRITE_OID_FIELD(constcollid);
  WRITE_INT_FIELD(constlen);
  WRITE_BOOL_FIELD(constbyval);
  WRITE_BOOL_FIELD(constisnull);
  WRITE_LOCATION_FIELD(location);

  appendStringInfoString(str, " :constvalue ");
  if (node->constisnull)
    appendStringInfoString(str, "<>");
  else
    _outDatum(str, node->constvalue, node->constlen, node->constbyval);
}

static void
_outParam(std::stringstream& str, const Param *node)
{
  WRITE_NODE_TYPE("PARAM");

  WRITE_ENUM_FIELD(paramkind, ParamKind);
  WRITE_INT_FIELD(paramid);
  WRITE_OID_FIELD(paramtype);
  WRITE_INT_FIELD(paramtypmod);
  WRITE_OID_FIELD(paramcollid);
  WRITE_LOCATION_FIELD(location);
}

static void
_outAggref(std::stringstream& str, const Aggref *node)
{
  WRITE_NODE_TYPE("AGGREF");

  WRITE_OID_FIELD(aggfnoid);
  WRITE_OID_FIELD(aggtype);
  WRITE_OID_FIELD(aggcollid);
  WRITE_OID_FIELD(inputcollid);
  WRITE_NODE_FIELD(aggdirectargs);
  WRITE_NODE_FIELD(args);
  WRITE_NODE_FIELD(aggorder);
  WRITE_NODE_FIELD(aggdistinct);
  WRITE_NODE_FIELD(aggfilter);
  WRITE_BOOL_FIELD(aggstar);
  WRITE_BOOL_FIELD(aggvariadic);
  WRITE_CHAR_FIELD(aggkind);
  WRITE_UINT_FIELD(agglevelsup);
  WRITE_LOCATION_FIELD(location);
}

static void
_outGroupingFunc(std::stringstream& str, const GroupingFunc *node)
{
  WRITE_NODE_TYPE("GROUPINGFUNC");

  WRITE_NODE_FIELD(args);
  WRITE_NODE_FIELD(refs);
  WRITE_NODE_FIELD(cols);
  WRITE_INT_FIELD(agglevelsup);
  WRITE_LOCATION_FIELD(location);
}

static void
_outWindowFunc(std::stringstream& str, const WindowFunc *node)
{
  WRITE_NODE_TYPE("WINDOWFUNC");

  WRITE_OID_FIELD(winfnoid);
  WRITE_OID_FIELD(wintype);
  WRITE_OID_FIELD(wincollid);
  WRITE_OID_FIELD(inputcollid);
  WRITE_NODE_FIELD(args);
  WRITE_NODE_FIELD(aggfilter);
  WRITE_UINT_FIELD(winref);
  WRITE_BOOL_FIELD(winstar);
  WRITE_BOOL_FIELD(winagg);
  WRITE_LOCATION_FIELD(location);
}

static void
_outArrayRef(std::stringstream& str, const ArrayRef *node)
{
  WRITE_NODE_TYPE("ARRAYREF");

  WRITE_OID_FIELD(refarraytype);
  WRITE_OID_FIELD(refelemtype);
  WRITE_INT_FIELD(reftypmod);
  WRITE_OID_FIELD(refcollid);
  WRITE_NODE_FIELD(refupperindexpr);
  WRITE_NODE_FIELD(reflowerindexpr);
  WRITE_NODE_FIELD(refexpr);
  WRITE_NODE_FIELD(refassgnexpr);
}

static void
_outFuncExpr(std::stringstream& str, const FuncExpr *node)
{
  WRITE_NODE_TYPE("FUNCEXPR");

  WRITE_OID_FIELD(funcid);
  WRITE_OID_FIELD(funcresulttype);
  WRITE_BOOL_FIELD(funcretset);
  WRITE_BOOL_FIELD(funcvariadic);
  WRITE_ENUM_FIELD(funcformat, CoercionForm);
  WRITE_OID_FIELD(funccollid);
  WRITE_OID_FIELD(inputcollid);
  WRITE_NODE_FIELD(args);
  WRITE_LOCATION_FIELD(location);
}

static void
_outNamedArgExpr(std::stringstream& str, const NamedArgExpr *node)
{
  WRITE_NODE_TYPE("NAMEDARGEXPR");

  WRITE_NODE_FIELD(arg);
  WRITE_STRING_FIELD(name);
  WRITE_INT_FIELD(argnumber);
  WRITE_LOCATION_FIELD(location);
}

static void
_outOpExpr(std::stringstream& str, const OpExpr *node)
{
  WRITE_NODE_TYPE("OPEXPR");

  WRITE_OID_FIELD(opno);
  WRITE_OID_FIELD(opfuncid);
  WRITE_OID_FIELD(opresulttype);
  WRITE_BOOL_FIELD(opretset);
  WRITE_OID_FIELD(opcollid);
  WRITE_OID_FIELD(inputcollid);
  WRITE_NODE_FIELD(args);
  WRITE_LOCATION_FIELD(location);
}

static void
_outDistinctExpr(std::stringstream& str, const DistinctExpr *node)
{
  WRITE_NODE_TYPE("DISTINCTEXPR");

  WRITE_OID_FIELD(opno);
  WRITE_OID_FIELD(opfuncid);
  WRITE_OID_FIELD(opresulttype);
  WRITE_BOOL_FIELD(opretset);
  WRITE_OID_FIELD(opcollid);
  WRITE_OID_FIELD(inputcollid);
  WRITE_NODE_FIELD(args);
  WRITE_LOCATION_FIELD(location);
}

static void
_outNullIfExpr(std::stringstream& str, const NullIfExpr *node)
{
  WRITE_NODE_TYPE("NULLIFEXPR");

  WRITE_OID_FIELD(opno);
  WRITE_OID_FIELD(opfuncid);
  WRITE_OID_FIELD(opresulttype);
  WRITE_BOOL_FIELD(opretset);
  WRITE_OID_FIELD(opcollid);
  WRITE_OID_FIELD(inputcollid);
  WRITE_NODE_FIELD(args);
  WRITE_LOCATION_FIELD(location);
}

static void
_outScalarArrayOpExpr(std::stringstream& str, const ScalarArrayOpExpr *node)
{
  WRITE_NODE_TYPE("SCALARARRAYOPEXPR");

  WRITE_OID_FIELD(opno);
  WRITE_OID_FIELD(opfuncid);
  WRITE_BOOL_FIELD(useOr);
  WRITE_OID_FIELD(inputcollid);
  WRITE_NODE_FIELD(args);
  WRITE_LOCATION_FIELD(location);
}

static void
_outBoolExpr(std::stringstream& str, const BoolExpr *node)
{
  const char	   *opstr = NULL;

  WRITE_NODE_TYPE("BOOLEXPR");

  /* do-it-yourself enum representation */
  switch (node->boolop)
  {
    case AND_EXPR:
      opstr = std::string("and").c_str();
      break;
    case OR_EXPR:
      opstr = std::string("or").c_str();
      break;
    case NOT_EXPR:
      opstr = std::string("not").c_str();
      break;
  }
  appendStringInfoString(str, " :boolop ");
  _outToken(str, opstr);

  WRITE_NODE_FIELD(args);
  WRITE_LOCATION_FIELD(location);
}

static void
_outSubLink(std::stringstream& str, const SubLink *node)
{
  WRITE_NODE_TYPE("SUBLINK");

  WRITE_ENUM_FIELD(subLinkType, SubLinkType);
  WRITE_INT_FIELD(subLinkId);
  WRITE_NODE_FIELD(testexpr);
  WRITE_NODE_FIELD(operName);
  WRITE_NODE_FIELD(subselect);
  WRITE_LOCATION_FIELD(location);
}

static void
_outSubPlan(std::stringstream& str, const SubPlan *node)
{
  WRITE_NODE_TYPE("SUBPLAN");

  WRITE_ENUM_FIELD(subLinkType, SubLinkType);
  WRITE_NODE_FIELD(testexpr);
  WRITE_NODE_FIELD(paramIds);
  WRITE_INT_FIELD(plan_id);
  WRITE_STRING_FIELD(plan_name);
  WRITE_OID_FIELD(firstColType);
  WRITE_INT_FIELD(firstColTypmod);
  WRITE_OID_FIELD(firstColCollation);
  WRITE_BOOL_FIELD(useHashTable);
  WRITE_BOOL_FIELD(unknownEqFalse);
  WRITE_NODE_FIELD(setParam);
  WRITE_NODE_FIELD(parParam);
  WRITE_NODE_FIELD(args);
  WRITE_FLOAT_FIELD(startup_cost, "%.2f");
  WRITE_FLOAT_FIELD(per_call_cost, "%.2f");
}

static void
_outAlternativeSubPlan(std::stringstream& str, const AlternativeSubPlan *node)
{
  WRITE_NODE_TYPE("ALTERNATIVESUBPLAN");

  WRITE_NODE_FIELD(subplans);
}

static void
_outFieldSelect(std::stringstream& str, const FieldSelect *node)
{
  WRITE_NODE_TYPE("FIELDSELECT");

  WRITE_NODE_FIELD(arg);
  WRITE_INT_FIELD(fieldnum);
  WRITE_OID_FIELD(resulttype);
  WRITE_INT_FIELD(resulttypmod);
  WRITE_OID_FIELD(resultcollid);
}

static void
_outFieldStore(std::stringstream& str, const FieldStore *node)
{
  WRITE_NODE_TYPE("FIELDSTORE");

  WRITE_NODE_FIELD(arg);
  WRITE_NODE_FIELD(newvals);
  WRITE_NODE_FIELD(fieldnums);
  WRITE_OID_FIELD(resulttype);
}

static void
_outRelabelType(std::stringstream& str, const RelabelType *node)
{
  WRITE_NODE_TYPE("RELABELTYPE");

  WRITE_NODE_FIELD(arg);
  WRITE_OID_FIELD(resulttype);
  WRITE_INT_FIELD(resulttypmod);
  WRITE_OID_FIELD(resultcollid);
  WRITE_ENUM_FIELD(relabelformat, CoercionForm);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceViaIO(std::stringstream& str, const CoerceViaIO *node)
{
  WRITE_NODE_TYPE("COERCEVIAIO");

  WRITE_NODE_FIELD(arg);
  WRITE_OID_FIELD(resulttype);
  WRITE_OID_FIELD(resultcollid);
  WRITE_ENUM_FIELD(coerceformat, CoercionForm);
  WRITE_LOCATION_FIELD(location);
}

static void
_outArrayCoerceExpr(std::stringstream& str, const ArrayCoerceExpr *node)
{
  WRITE_NODE_TYPE("ARRAYCOERCEEXPR");

  WRITE_NODE_FIELD(arg);
  WRITE_OID_FIELD(elemfuncid);
  WRITE_OID_FIELD(resulttype);
  WRITE_INT_FIELD(resulttypmod);
  WRITE_OID_FIELD(resultcollid);
  WRITE_BOOL_FIELD(isExplicit);
  WRITE_ENUM_FIELD(coerceformat, CoercionForm);
  WRITE_LOCATION_FIELD(location);
}

static void
_outConvertRowtypeExpr(std::stringstream& str, const ConvertRowtypeExpr *node)
{
  WRITE_NODE_TYPE("CONVERTROWTYPEEXPR");

  WRITE_NODE_FIELD(arg);
  WRITE_OID_FIELD(resulttype);
  WRITE_ENUM_FIELD(convertformat, CoercionForm);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCollateExpr(std::stringstream& str, const CollateExpr *node)
{
  WRITE_NODE_TYPE("COLLATE");

  WRITE_NODE_FIELD(arg);
  WRITE_OID_FIELD(collOid);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCaseExpr(std::stringstream& str, const CaseExpr *node)
{
  WRITE_NODE_TYPE("CASE");

  WRITE_OID_FIELD(casetype);
  WRITE_OID_FIELD(casecollid);
  WRITE_NODE_FIELD(arg);
  WRITE_NODE_FIELD(args);
  WRITE_NODE_FIELD(defresult);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCaseWhen(std::stringstream& str, const CaseWhen *node)
{
  WRITE_NODE_TYPE("WHEN");

  WRITE_NODE_FIELD(expr);
  WRITE_NODE_FIELD(result);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCaseTestExpr(std::stringstream& str, const CaseTestExpr *node)
{
  WRITE_NODE_TYPE("CASETESTEXPR");

  WRITE_OID_FIELD(typeId);
  WRITE_INT_FIELD(typeMod);
  WRITE_OID_FIELD(collation);
}

static void
_outArrayExpr(std::stringstream& str, const ArrayExpr *node)
{
  WRITE_NODE_TYPE("ARRAY");

  WRITE_OID_FIELD(array_typeid);
  WRITE_OID_FIELD(array_collid);
  WRITE_OID_FIELD(element_typeid);
  WRITE_NODE_FIELD(elements);
  WRITE_BOOL_FIELD(multidims);
  WRITE_LOCATION_FIELD(location);
}

static void
_outRowExpr(std::stringstream& str, const RowExpr *node)
{
  WRITE_NODE_TYPE("ROW");

  WRITE_NODE_FIELD(args);
  WRITE_OID_FIELD(row_typeid);
  WRITE_ENUM_FIELD(row_format, CoercionForm);
  WRITE_NODE_FIELD(colnames);
  WRITE_LOCATION_FIELD(location);
}

static void
_outRowCompareExpr(std::stringstream& str, const RowCompareExpr *node)
{
  WRITE_NODE_TYPE("ROWCOMPARE");

  WRITE_ENUM_FIELD(rctype, RowCompareType);
  WRITE_NODE_FIELD(opnos);
  WRITE_NODE_FIELD(opfamilies);
  WRITE_NODE_FIELD(inputcollids);
  WRITE_NODE_FIELD(largs);
  WRITE_NODE_FIELD(rargs);
}

static void
_outCoalesceExpr(std::stringstream& str, const CoalesceExpr *node)
{
  WRITE_NODE_TYPE("COALESCE");

  WRITE_OID_FIELD(coalescetype);
  WRITE_OID_FIELD(coalescecollid);
  WRITE_NODE_FIELD(args);
  WRITE_LOCATION_FIELD(location);
}

static void
_outMinMaxExpr(std::stringstream& str, const MinMaxExpr *node)
{
  WRITE_NODE_TYPE("MINMAX");

  WRITE_OID_FIELD(minmaxtype);
  WRITE_OID_FIELD(minmaxcollid);
  WRITE_OID_FIELD(inputcollid);
  WRITE_ENUM_FIELD(op, MinMaxOp);
  WRITE_NODE_FIELD(args);
  WRITE_LOCATION_FIELD(location);
}

static void
_outXmlExpr(std::stringstream& str, const XmlExpr *node)
{
  WRITE_NODE_TYPE("XMLEXPR");

  WRITE_ENUM_FIELD(op, XmlExprOp);
  WRITE_STRING_FIELD(name);
  WRITE_NODE_FIELD(named_args);
  WRITE_NODE_FIELD(arg_names);
  WRITE_NODE_FIELD(args);
  WRITE_ENUM_FIELD(xmloption, XmlOptionType);
  WRITE_OID_FIELD(type);
  WRITE_INT_FIELD(typmod);
  WRITE_LOCATION_FIELD(location);
}

static void
_outNullTest(std::stringstream& str, const NullTest *node)
{
  WRITE_NODE_TYPE("NULLTEST");

  WRITE_NODE_FIELD(arg);
  WRITE_ENUM_FIELD(nulltesttype, NullTestType);
  WRITE_BOOL_FIELD(argisrow);
  WRITE_LOCATION_FIELD(location);
}

static void
_outBooleanTest(std::stringstream& str, const BooleanTest *node)
{
  WRITE_NODE_TYPE("BOOLEANTEST");

  WRITE_NODE_FIELD(arg);
  WRITE_ENUM_FIELD(booltesttype, BoolTestType);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceToDomain(std::stringstream& str, const CoerceToDomain *node)
{
  WRITE_NODE_TYPE("COERCETODOMAIN");

  WRITE_NODE_FIELD(arg);
  WRITE_OID_FIELD(resulttype);
  WRITE_INT_FIELD(resulttypmod);
  WRITE_OID_FIELD(resultcollid);
  WRITE_ENUM_FIELD(coercionformat, CoercionForm);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceToDomainValue(std::stringstream& str, const CoerceToDomainValue *node)
{
  WRITE_NODE_TYPE("COERCETODOMAINVALUE");

  WRITE_OID_FIELD(typeId);
  WRITE_INT_FIELD(typeMod);
  WRITE_OID_FIELD(collation);
  WRITE_LOCATION_FIELD(location);
}

static void
_outSetToDefault(std::stringstream& str, const SetToDefault *node)
{
  WRITE_NODE_TYPE("SETTODEFAULT");

  WRITE_OID_FIELD(typeId);
  WRITE_INT_FIELD(typeMod);
  WRITE_OID_FIELD(collation);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCurrentOfExpr(std::stringstream& str, const CurrentOfExpr *node)
{
  WRITE_NODE_TYPE("CURRENTOFEXPR");

  WRITE_UINT_FIELD(cvarno);
  WRITE_STRING_FIELD(cursor_name);
  WRITE_INT_FIELD(cursor_param);
}

static void
_outInferenceElem(std::stringstream& str, const InferenceElem *node)
{
  WRITE_NODE_TYPE("INFERENCEELEM");

  WRITE_NODE_FIELD(expr);
  WRITE_OID_FIELD(infercollid);
  WRITE_OID_FIELD(inferopclass);
}

static void
_outTargetEntry(std::stringstream& str, const TargetEntry *node)
{
  WRITE_NODE_TYPE("TARGETENTRY");

  WRITE_NODE_FIELD(expr);
  WRITE_INT_FIELD(resno);
  WRITE_STRING_FIELD(resname);
  WRITE_UINT_FIELD(ressortgroupref);
  WRITE_OID_FIELD(resorigtbl);
  WRITE_INT_FIELD(resorigcol);
  WRITE_BOOL_FIELD(resjunk);
}

static void
_outRangeTblRef(std::stringstream& str, const RangeTblRef *node)
{
  WRITE_NODE_TYPE("RANGETBLREF");

  WRITE_INT_FIELD(rtindex);
}

static void
_outJoinExpr(std::stringstream& str, const JoinExpr *node)
{
  WRITE_NODE_TYPE("JOINEXPR");

  WRITE_ENUM_FIELD(jointype, JoinType);
  WRITE_BOOL_FIELD(isNatural);
  WRITE_NODE_FIELD(larg);
  WRITE_NODE_FIELD(rarg);
  WRITE_NODE_FIELD(usingClause);
  WRITE_NODE_FIELD(quals);
  WRITE_NODE_FIELD(alias);
  WRITE_INT_FIELD(rtindex);
}

static void
_outFromExpr(std::stringstream& str, const FromExpr *node)
{
  WRITE_NODE_TYPE("FROMEXPR");

  WRITE_NODE_FIELD(fromlist);
  WRITE_NODE_FIELD(quals);
}

static void
_outOnConflictExpr(std::stringstream& str, const OnConflictExpr *node)
{
  WRITE_NODE_TYPE("ONCONFLICTEXPR");

  WRITE_ENUM_FIELD(action, OnConflictAction);
  WRITE_NODE_FIELD(arbiterElems);
  WRITE_NODE_FIELD(arbiterWhere);
  WRITE_NODE_FIELD(onConflictSet);
  WRITE_NODE_FIELD(onConflictWhere);
  WRITE_OID_FIELD(constraint);
  WRITE_INT_FIELD(exclRelIndex);
  WRITE_NODE_FIELD(exclRelTlist);
}

/*****************************************************************************
 *
 *	Stuff from relation.h.
 *
 *****************************************************************************/

/*
 * print the basic stuff of all nodes that inherit from Path
 *
 * Note we do NOT print the parent, else we'd be in infinite recursion.
 * We can print the parent's relids for identification purposes, though.
 * We also do not print the whole of param_info, since it's printed by
 * _outRelOptInfo; it's sufficient and less cluttering to print just the
 * required outer relids.
 */
static void
_outPathInfo(std::stringstream& str, const Path *node)
{
  WRITE_ENUM_FIELD(pathtype, NodeTag);
  appendStringInfoString(str, " :parent_relids ");
  if (node->parent)
    _outBitmapset(str, node->parent->relids);
  else
    _outBitmapset(str, NULL);
  appendStringInfoString(str, " :required_outer ");
  if (node->param_info)
    _outBitmapset(str, node->param_info->ppi_req_outer);
  else
    _outBitmapset(str, NULL);
  WRITE_FLOAT_FIELD(rows, "%.0f");
  WRITE_FLOAT_FIELD(startup_cost, "%.2f");
  WRITE_FLOAT_FIELD(total_cost, "%.2f");
  WRITE_NODE_FIELD(pathkeys);
}

/*
 * print the basic stuff of all nodes that inherit from JoinPath
 */
static void
_outJoinPathInfo(std::stringstream& str, const JoinPath *node)
{
  _outPathInfo(str, (const Path *) node);

  WRITE_ENUM_FIELD(jointype, JoinType);
  WRITE_NODE_FIELD(outerjoinpath);
  WRITE_NODE_FIELD(innerjoinpath);
  WRITE_NODE_FIELD(joinrestrictinfo);
}

static void
_outPath(std::stringstream& str, const Path *node)
{
  WRITE_NODE_TYPE("PATH");

  _outPathInfo(str, (const Path *) node);
}

static void
_outIndexPath(std::stringstream& str, const IndexPath *node)
{
  WRITE_NODE_TYPE("INDEXPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(indexinfo);
  WRITE_NODE_FIELD(indexclauses);
  WRITE_NODE_FIELD(indexquals);
  WRITE_NODE_FIELD(indexqualcols);
  WRITE_NODE_FIELD(indexorderbys);
  WRITE_NODE_FIELD(indexorderbycols);
  WRITE_ENUM_FIELD(indexscandir, ScanDirection);
  WRITE_FLOAT_FIELD(indextotalcost, "%.2f");
  WRITE_FLOAT_FIELD(indexselectivity, "%.4f");
}

static void
_outBitmapHeapPath(std::stringstream& str, const BitmapHeapPath *node)
{
  WRITE_NODE_TYPE("BITMAPHEAPPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(bitmapqual);
}

static void
_outBitmapAndPath(std::stringstream& str, const BitmapAndPath *node)
{
  WRITE_NODE_TYPE("BITMAPANDPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(bitmapquals);
  WRITE_FLOAT_FIELD(bitmapselectivity, "%.4f");
}

static void
_outBitmapOrPath(std::stringstream& str, const BitmapOrPath *node)
{
  WRITE_NODE_TYPE("BITMAPORPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(bitmapquals);
  WRITE_FLOAT_FIELD(bitmapselectivity, "%.4f");
}

static void
_outTidPath(std::stringstream& str, const TidPath *node)
{
  WRITE_NODE_TYPE("TIDPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(tidquals);
}

static void
_outForeignPath(std::stringstream& str, const ForeignPath *node)
{
  WRITE_NODE_TYPE("FOREIGNPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(fdw_private);
}

static void
_outCustomPath(std::stringstream& str, const CustomPath *node)
{
  WRITE_NODE_TYPE("CUSTOMPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_UINT_FIELD(flags);
  WRITE_NODE_FIELD(custom_private);
  appendStringInfoString(str, " :methods ");
  _outToken(str, node->methods->CustomName);

  // TODO: Fix conversions
  //if (node->methods->TextOutCustomPath)
  //	node->methods->TextOutCustomPath(str, node);
}

static void
_outAppendPath(std::stringstream& str, const AppendPath *node)
{
  WRITE_NODE_TYPE("APPENDPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(subpaths);
}

static void
_outMergeAppendPath(std::stringstream& str, const MergeAppendPath *node)
{
  WRITE_NODE_TYPE("MERGEAPPENDPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(subpaths);
  WRITE_FLOAT_FIELD(limit_tuples, "%.0f");
}

static void
_outResultPath(std::stringstream& str, const ResultPath *node)
{
  WRITE_NODE_TYPE("RESULTPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(quals);
}

static void
_outMaterialPath(std::stringstream& str, const MaterialPath *node)
{
  WRITE_NODE_TYPE("MATERIALPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(subpath);
}

static void
_outUniquePath(std::stringstream& str, const UniquePath *node)
{
  WRITE_NODE_TYPE("UNIQUEPATH");

  _outPathInfo(str, (const Path *) node);

  WRITE_NODE_FIELD(subpath);
  WRITE_ENUM_FIELD(umethod, UniquePathMethod);
  WRITE_NODE_FIELD(in_operators);
  WRITE_NODE_FIELD(uniq_exprs);
}

static void
_outNestPath(std::stringstream& str, const NestPath *node)
{
  WRITE_NODE_TYPE("NESTPATH");

  _outJoinPathInfo(str, (const JoinPath *) node);
}

static void
_outMergePath(std::stringstream& str, const MergePath *node)
{
  WRITE_NODE_TYPE("MERGEPATH");

  _outJoinPathInfo(str, (const JoinPath *) node);

  WRITE_NODE_FIELD(path_mergeclauses);
  WRITE_NODE_FIELD(outersortkeys);
  WRITE_NODE_FIELD(innersortkeys);
  WRITE_BOOL_FIELD(materialize_inner);
}

static void
_outHashPath(std::stringstream& str, const HashPath *node)
{
  WRITE_NODE_TYPE("HASHPATH");

  _outJoinPathInfo(str, (const JoinPath *) node);

  WRITE_NODE_FIELD(path_hashclauses);
  WRITE_INT_FIELD(num_batches);
}

static void
_outPlannerGlobal(std::stringstream& str, const PlannerGlobal *node)
{
  WRITE_NODE_TYPE("PLANNERGLOBAL");

  /* NB: this isn't a complete set of fields */
  WRITE_NODE_FIELD(subplans);
  WRITE_BITMAPSET_FIELD(rewindPlanIDs);
  WRITE_NODE_FIELD(finalrtable);
  WRITE_NODE_FIELD(finalrowmarks);
  WRITE_NODE_FIELD(resultRelations);
  WRITE_NODE_FIELD(relationOids);
  WRITE_NODE_FIELD(invalItems);
  WRITE_INT_FIELD(nParamExec);
  WRITE_UINT_FIELD(lastPHId);
  WRITE_UINT_FIELD(lastRowMarkId);
  WRITE_BOOL_FIELD(transientPlan);
  WRITE_BOOL_FIELD(hasRowSecurity);
}

static void
_outPlannerInfo(std::stringstream& str, const PlannerInfo *node)
{
  WRITE_NODE_TYPE("PLANNERINFO");

  /* NB: this isn't a complete set of fields */
  WRITE_NODE_FIELD(parse);
  WRITE_NODE_FIELD(glob);
  WRITE_UINT_FIELD(query_level);
  WRITE_NODE_FIELD(plan_params);
  WRITE_BITMAPSET_FIELD(all_baserels);
  WRITE_BITMAPSET_FIELD(nullable_baserels);
  WRITE_NODE_FIELD(join_rel_list);
  WRITE_INT_FIELD(join_cur_level);
  WRITE_NODE_FIELD(init_plans);
  WRITE_NODE_FIELD(cte_plan_ids);
  WRITE_NODE_FIELD(multiexpr_params);
  WRITE_NODE_FIELD(eq_classes);
  WRITE_NODE_FIELD(canon_pathkeys);
  WRITE_NODE_FIELD(left_join_clauses);
  WRITE_NODE_FIELD(right_join_clauses);
  WRITE_NODE_FIELD(full_join_clauses);
  WRITE_NODE_FIELD(join_info_list);
  WRITE_NODE_FIELD(lateral_info_list);
  WRITE_NODE_FIELD(append_rel_list);
  WRITE_NODE_FIELD(rowMarks);
  WRITE_NODE_FIELD(placeholder_list);
  WRITE_NODE_FIELD(query_pathkeys);
  WRITE_NODE_FIELD(group_pathkeys);
  WRITE_NODE_FIELD(window_pathkeys);
  WRITE_NODE_FIELD(distinct_pathkeys);
  WRITE_NODE_FIELD(sort_pathkeys);
  WRITE_NODE_FIELD(minmax_aggs);
  WRITE_FLOAT_FIELD(total_table_pages, "%.0f");
  WRITE_FLOAT_FIELD(tuple_fraction, "%.4f");
  WRITE_FLOAT_FIELD(limit_tuples, "%.0f");
  WRITE_BOOL_FIELD(hasInheritedTarget);
  WRITE_BOOL_FIELD(hasJoinRTEs);
  WRITE_BOOL_FIELD(hasLateralRTEs);
  WRITE_BOOL_FIELD(hasDeletedRTEs);
  WRITE_BOOL_FIELD(hasHavingQual);
  WRITE_BOOL_FIELD(hasPseudoConstantQuals);
  WRITE_BOOL_FIELD(hasRecursion);
  WRITE_INT_FIELD(wt_param_id);
  WRITE_BITMAPSET_FIELD(curOuterRels);
  WRITE_NODE_FIELD(curOuterParams);
}

static void
_outRelOptInfo(std::stringstream& str, const RelOptInfo *node)
{
  WRITE_NODE_TYPE("RELOPTINFO");

  /* NB: this isn't a complete set of fields */
  WRITE_ENUM_FIELD(reloptkind, RelOptKind);
  WRITE_BITMAPSET_FIELD(relids);
  WRITE_FLOAT_FIELD(rows, "%.0f");
  WRITE_INT_FIELD(width);
  WRITE_BOOL_FIELD(consider_startup);
  WRITE_NODE_FIELD(reltargetlist);
  WRITE_NODE_FIELD(pathlist);
  WRITE_NODE_FIELD(ppilist);
  WRITE_NODE_FIELD(cheapest_startup_path);
  WRITE_NODE_FIELD(cheapest_total_path);
  WRITE_NODE_FIELD(cheapest_unique_path);
  WRITE_NODE_FIELD(cheapest_parameterized_paths);
  WRITE_UINT_FIELD(relid);
  WRITE_OID_FIELD(reltablespace);
  WRITE_ENUM_FIELD(rtekind, RTEKind);
  WRITE_INT_FIELD(min_attr);
  WRITE_INT_FIELD(max_attr);
  WRITE_NODE_FIELD(lateral_vars);
  WRITE_BITMAPSET_FIELD(lateral_relids);
  WRITE_BITMAPSET_FIELD(lateral_referencers);
  WRITE_NODE_FIELD(indexlist);
  WRITE_UINT_FIELD(pages);
  WRITE_FLOAT_FIELD(tuples, "%.0f");
  WRITE_FLOAT_FIELD(allvisfrac, "%.6f");
  WRITE_NODE_FIELD(subplan);
  WRITE_NODE_FIELD(subroot);
  WRITE_NODE_FIELD(subplan_params);
  WRITE_OID_FIELD(serverid);
  /* we don't try to print fdwroutine or fdw_private */
  WRITE_NODE_FIELD(baserestrictinfo);
  WRITE_NODE_FIELD(joininfo);
  WRITE_BOOL_FIELD(has_eclass_joins);
}

static void
_outIndexOptInfo(std::stringstream& str, const IndexOptInfo *node)
{
  WRITE_NODE_TYPE("INDEXOPTINFO");

  /* NB: this isn't a complete set of fields */
  WRITE_OID_FIELD(indexoid);
  /* Do NOT print rel field, else infinite recursion */
  WRITE_UINT_FIELD(pages);
  WRITE_FLOAT_FIELD(tuples, "%.0f");
  WRITE_INT_FIELD(tree_height);
  WRITE_INT_FIELD(ncolumns);
  /* array fields aren't really worth the trouble to print */
  WRITE_OID_FIELD(relam);
  /* indexprs is redundant since we print indextlist */
  WRITE_NODE_FIELD(indpred);
  WRITE_NODE_FIELD(indextlist);
  WRITE_BOOL_FIELD(predOK);
  WRITE_BOOL_FIELD(unique);
  WRITE_BOOL_FIELD(immediate);
  WRITE_BOOL_FIELD(hypothetical);
  /* we don't bother with fields copied from the pg_am entry */
}

static void
_outEquivalenceClass(std::stringstream& str, const EquivalenceClass *node)
{
  /*
   * To simplify reading, we just chase up to the topmost merged EC and
   * print that, without bothering to show the merge-ees separately.
   */
  while (node->ec_merged)
    node = node->ec_merged;

  WRITE_NODE_TYPE("EQUIVALENCECLASS");

  WRITE_NODE_FIELD(ec_opfamilies);
  WRITE_OID_FIELD(ec_collation);
  WRITE_NODE_FIELD(ec_members);
  WRITE_NODE_FIELD(ec_sources);
  WRITE_NODE_FIELD(ec_derives);
  WRITE_BITMAPSET_FIELD(ec_relids);
  WRITE_BOOL_FIELD(ec_has_const);
  WRITE_BOOL_FIELD(ec_has_volatile);
  WRITE_BOOL_FIELD(ec_below_outer_join);
  WRITE_BOOL_FIELD(ec_broken);
  WRITE_UINT_FIELD(ec_sortref);
}

static void
_outEquivalenceMember(std::stringstream& str, const EquivalenceMember *node)
{
  WRITE_NODE_TYPE("EQUIVALENCEMEMBER");

  WRITE_NODE_FIELD(em_expr);
  WRITE_BITMAPSET_FIELD(em_relids);
  WRITE_BITMAPSET_FIELD(em_nullable_relids);
  WRITE_BOOL_FIELD(em_is_const);
  WRITE_BOOL_FIELD(em_is_child);
  WRITE_OID_FIELD(em_datatype);
}

static void
_outPathKey(std::stringstream& str, const PathKey *node)
{
  WRITE_NODE_TYPE("PATHKEY");

  WRITE_NODE_FIELD(pk_eclass);
  WRITE_OID_FIELD(pk_opfamily);
  WRITE_INT_FIELD(pk_strategy);
  WRITE_BOOL_FIELD(pk_nulls_first);
}

static void
_outParamPathInfo(std::stringstream& str, const ParamPathInfo *node)
{
  WRITE_NODE_TYPE("PARAMPATHINFO");

  WRITE_BITMAPSET_FIELD(ppi_req_outer);
  WRITE_FLOAT_FIELD(ppi_rows, "%.0f");
  WRITE_NODE_FIELD(ppi_clauses);
}

static void
_outRestrictInfo(std::stringstream& str, const RestrictInfo *node)
{
  WRITE_NODE_TYPE("RESTRICTINFO");

  /* NB: this isn't a complete set of fields */
  WRITE_NODE_FIELD(clause);
  WRITE_BOOL_FIELD(is_pushed_down);
  WRITE_BOOL_FIELD(outerjoin_delayed);
  WRITE_BOOL_FIELD(can_join);
  WRITE_BOOL_FIELD(pseudoconstant);
  WRITE_BITMAPSET_FIELD(clause_relids);
  WRITE_BITMAPSET_FIELD(required_relids);
  WRITE_BITMAPSET_FIELD(outer_relids);
  WRITE_BITMAPSET_FIELD(nullable_relids);
  WRITE_BITMAPSET_FIELD(left_relids);
  WRITE_BITMAPSET_FIELD(right_relids);
  WRITE_NODE_FIELD(orclause);
  /* don't write parent_ec, leads to infinite recursion in plan tree dump */
  WRITE_FLOAT_FIELD(norm_selec, "%.4f");
  WRITE_FLOAT_FIELD(outer_selec, "%.4f");
  WRITE_NODE_FIELD(mergeopfamilies);
  /* don't write left_ec, leads to infinite recursion in plan tree dump */
  /* don't write right_ec, leads to infinite recursion in plan tree dump */
  WRITE_NODE_FIELD(left_em);
  WRITE_NODE_FIELD(right_em);
  WRITE_BOOL_FIELD(outer_is_left);
  WRITE_OID_FIELD(hashjoinoperator);
}

static void
_outPlaceHolderVar(std::stringstream& str, const PlaceHolderVar *node)
{
  WRITE_NODE_TYPE("PLACEHOLDERVAR");

  WRITE_NODE_FIELD(phexpr);
  WRITE_BITMAPSET_FIELD(phrels);
  WRITE_UINT_FIELD(phid);
  WRITE_UINT_FIELD(phlevelsup);
}

static void
_outSpecialJoinInfo(std::stringstream& str, const SpecialJoinInfo *node)
{
  WRITE_NODE_TYPE("SPECIALJOININFO");

  WRITE_BITMAPSET_FIELD(min_lefthand);
  WRITE_BITMAPSET_FIELD(min_righthand);
  WRITE_BITMAPSET_FIELD(syn_lefthand);
  WRITE_BITMAPSET_FIELD(syn_righthand);
  WRITE_ENUM_FIELD(jointype, JoinType);
  WRITE_BOOL_FIELD(lhs_strict);
  WRITE_BOOL_FIELD(delay_upper_joins);
  WRITE_BOOL_FIELD(semi_can_btree);
  WRITE_BOOL_FIELD(semi_can_hash);
  WRITE_NODE_FIELD(semi_operators);
  WRITE_NODE_FIELD(semi_rhs_exprs);
}

static void
_outLateralJoinInfo(std::stringstream& str, const LateralJoinInfo *node)
{
  WRITE_NODE_TYPE("LATERALJOININFO");

  WRITE_BITMAPSET_FIELD(lateral_lhs);
  WRITE_BITMAPSET_FIELD(lateral_rhs);
}

static void
_outAppendRelInfo(std::stringstream& str, const AppendRelInfo *node)
{
  WRITE_NODE_TYPE("APPENDRELINFO");

  WRITE_UINT_FIELD(parent_relid);
  WRITE_UINT_FIELD(child_relid);
  WRITE_OID_FIELD(parent_reltype);
  WRITE_OID_FIELD(child_reltype);
  WRITE_NODE_FIELD(translated_vars);
  WRITE_OID_FIELD(parent_reloid);
}

static void
_outPlaceHolderInfo(std::stringstream& str, const PlaceHolderInfo *node)
{
  WRITE_NODE_TYPE("PLACEHOLDERINFO");

  WRITE_UINT_FIELD(phid);
  WRITE_NODE_FIELD(ph_var);
  WRITE_BITMAPSET_FIELD(ph_eval_at);
  WRITE_BITMAPSET_FIELD(ph_lateral);
  WRITE_BITMAPSET_FIELD(ph_needed);
  WRITE_INT_FIELD(ph_width);
}

static void
_outMinMaxAggInfo(std::stringstream& str, const MinMaxAggInfo *node)
{
  WRITE_NODE_TYPE("MINMAXAGGINFO");

  WRITE_OID_FIELD(aggfnoid);
  WRITE_OID_FIELD(aggsortop);
  WRITE_NODE_FIELD(target);
  /* We intentionally omit subroot --- too large, not interesting enough */
  WRITE_NODE_FIELD(path);
  WRITE_FLOAT_FIELD(pathcost, "%.2f");
  WRITE_NODE_FIELD(param);
}

static void
_outPlannerParamItem(std::stringstream& str, const PlannerParamItem *node)
{
  WRITE_NODE_TYPE("PLANNERPARAMITEM");

  WRITE_NODE_FIELD(item);
  WRITE_INT_FIELD(paramId);
}

/*****************************************************************************
 *
 *	Stuff from parsenodes.h.
 *
 *****************************************************************************/

/*
 * print the basic stuff of all nodes that inherit from CreateStmt
 */
static void
_outCreateStmtInfo(std::stringstream& str, const CreateStmt *node)
{
  WRITE_NODE_FIELD(relation);
  WRITE_NODE_FIELD(tableElts);
  WRITE_NODE_FIELD(inhRelations);
  WRITE_NODE_FIELD(ofTypename);
  WRITE_NODE_FIELD(constraints);
  WRITE_NODE_FIELD(options);
  WRITE_ENUM_FIELD(oncommit, OnCommitAction);
  WRITE_STRING_FIELD(tablespacename);
  WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outCreateStmt(std::stringstream& str, const CreateStmt *node)
{
  WRITE_NODE_TYPE("CREATESTMT");

  _outCreateStmtInfo(str, (const CreateStmt *) node);
}

static void
_outCreateForeignTableStmt(std::stringstream& str, const CreateForeignTableStmt *node)
{
  WRITE_NODE_TYPE("CREATEFOREIGNTABLESTMT");

  _outCreateStmtInfo(str, (const CreateStmt *) node);

  WRITE_STRING_FIELD(servername);
  WRITE_NODE_FIELD(options);
}

static void
_outImportForeignSchemaStmt(std::stringstream& str, const ImportForeignSchemaStmt *node)
{
  WRITE_NODE_TYPE("IMPORTFOREIGNSCHEMASTMT");

  WRITE_STRING_FIELD(server_name);
  WRITE_STRING_FIELD(remote_schema);
  WRITE_STRING_FIELD(local_schema);
  WRITE_ENUM_FIELD(list_type, ImportForeignSchemaType);
  WRITE_NODE_FIELD(table_list);
  WRITE_NODE_FIELD(options);
}

static void
_outIndexStmt(std::stringstream& str, const IndexStmt *node)
{
  WRITE_NODE_TYPE("INDEXSTMT");

  WRITE_STRING_FIELD(idxname);
  WRITE_NODE_FIELD(relation);
  WRITE_STRING_FIELD(accessMethod);
  WRITE_STRING_FIELD(tableSpace);
  WRITE_NODE_FIELD(indexParams);
  WRITE_NODE_FIELD(options);
  WRITE_NODE_FIELD(whereClause);
  WRITE_NODE_FIELD(excludeOpNames);
  WRITE_STRING_FIELD(idxcomment);
  WRITE_OID_FIELD(indexOid);
  WRITE_OID_FIELD(oldNode);
  WRITE_BOOL_FIELD(unique);
  WRITE_BOOL_FIELD(primary);
  WRITE_BOOL_FIELD(isconstraint);
  WRITE_BOOL_FIELD(deferrable);
  WRITE_BOOL_FIELD(initdeferred);
  WRITE_BOOL_FIELD(transformed);
  WRITE_BOOL_FIELD(concurrent);
  WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outNotifyStmt(std::stringstream& str, const NotifyStmt *node)
{
  WRITE_NODE_TYPE("NOTIFY");

  WRITE_STRING_FIELD(conditionname);
  WRITE_STRING_FIELD(payload);
}

static void
_outDeclareCursorStmt(std::stringstream& str, const DeclareCursorStmt *node)
{
  WRITE_NODE_TYPE("DECLARECURSOR");

  WRITE_STRING_FIELD(portalname);
  WRITE_INT_FIELD(options);
  WRITE_NODE_FIELD(query);
}

static void
_outSelectStmt(std::stringstream& str, const SelectStmt *node)
{
  WRITE_NODE_TYPE("SELECT");

  WRITE_NODE_FIELD(distinctClause);
  WRITE_NODE_FIELD(intoClause);
  WRITE_NODE_FIELD(targetList);
  WRITE_NODE_FIELD(fromClause);
  WRITE_NODE_FIELD(whereClause);
  WRITE_NODE_FIELD(groupClause);
  WRITE_NODE_FIELD(havingClause);
  WRITE_NODE_FIELD(windowClause);
  WRITE_NODE_FIELD(valuesLists);
  WRITE_NODE_FIELD(sortClause);
  WRITE_NODE_FIELD(limitOffset);
  WRITE_NODE_FIELD(limitCount);
  WRITE_NODE_FIELD(lockingClause);
  WRITE_NODE_FIELD(withClause);
  WRITE_ENUM_FIELD(op, SetOperation);
  WRITE_BOOL_FIELD(all);
  WRITE_NODE_FIELD(larg);
  WRITE_NODE_FIELD(rarg);
}

static void
_outFuncCall(std::stringstream& str, const FuncCall *node)
{
  WRITE_NODE_TYPE("FUNCCALL");

  WRITE_NODE_FIELD(funcname);
  WRITE_NODE_FIELD(args);
  WRITE_NODE_FIELD(agg_order);
  WRITE_NODE_FIELD(agg_filter);
  WRITE_BOOL_FIELD(agg_within_group);
  WRITE_BOOL_FIELD(agg_star);
  WRITE_BOOL_FIELD(agg_distinct);
  WRITE_BOOL_FIELD(func_variadic);
  WRITE_NODE_FIELD(over);
  WRITE_LOCATION_FIELD(location);
}

static void
_outDefElem(std::stringstream& str, const DefElem *node)
{
  WRITE_NODE_TYPE("DEFELEM");

  WRITE_STRING_FIELD(defnamespace);
  WRITE_STRING_FIELD(defname);
  WRITE_NODE_FIELD(arg);
  WRITE_ENUM_FIELD(defaction, DefElemAction);
}

static void
_outTableLikeClause(std::stringstream& str, const TableLikeClause *node)
{
  WRITE_NODE_TYPE("TABLELIKECLAUSE");

  WRITE_NODE_FIELD(relation);
  WRITE_UINT_FIELD(options);
}

static void
_outLockingClause(std::stringstream& str, const LockingClause *node)
{
  WRITE_NODE_TYPE("LOCKINGCLAUSE");

  WRITE_NODE_FIELD(lockedRels);
  WRITE_ENUM_FIELD(strength, LockClauseStrength);
  WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
}

static void
_outXmlSerialize(std::stringstream& str, const XmlSerialize *node)
{
  WRITE_NODE_TYPE("XMLSERIALIZE");

  WRITE_ENUM_FIELD(xmloption, XmlOptionType);
  WRITE_NODE_FIELD(expr);
  WRITE_NODE_FIELD(typeName);
  WRITE_LOCATION_FIELD(location);
}

static void
_outColumnDef(std::stringstream& str, const ColumnDef *node)
{
  WRITE_NODE_TYPE("COLUMNDEF");

  WRITE_STRING_FIELD(colname);
  WRITE_NODE_FIELD(typeName);
  WRITE_INT_FIELD(inhcount);
  WRITE_BOOL_FIELD(is_local);
  WRITE_BOOL_FIELD(is_not_null);
  WRITE_BOOL_FIELD(is_from_type);
  WRITE_CHAR_FIELD(storage);
  WRITE_NODE_FIELD(raw_default);
  WRITE_NODE_FIELD(cooked_default);
  WRITE_NODE_FIELD(collClause);
  WRITE_OID_FIELD(collOid);
  WRITE_NODE_FIELD(constraints);
  WRITE_NODE_FIELD(fdwoptions);
  WRITE_LOCATION_FIELD(location);
}

static void
_outTypeName(std::stringstream& str, const TypeName *node)
{
  WRITE_NODE_TYPE("TYPENAME");

  WRITE_NODE_FIELD(names);
  WRITE_OID_FIELD(typeOid);
  WRITE_BOOL_FIELD(setof);
  WRITE_BOOL_FIELD(pct_type);
  WRITE_NODE_FIELD(typmods);
  WRITE_INT_FIELD(typemod);
  WRITE_NODE_FIELD(arrayBounds);
  WRITE_LOCATION_FIELD(location);
}

static void
_outTypeCast(std::stringstream& str, const TypeCast *node)
{
  WRITE_NODE_TYPE("TYPECAST");

  WRITE_NODE_FIELD(arg);
  WRITE_NODE_FIELD(typeName);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCollateClause(std::stringstream& str, const CollateClause *node)
{
  WRITE_NODE_TYPE("COLLATECLAUSE");

  WRITE_NODE_FIELD(arg);
  WRITE_NODE_FIELD(collname);
  WRITE_LOCATION_FIELD(location);
}

static void
_outIndexElem(std::stringstream& str, const IndexElem *node)
{
  WRITE_NODE_TYPE("INDEXELEM");

  WRITE_STRING_FIELD(name);
  WRITE_NODE_FIELD(expr);
  WRITE_STRING_FIELD(indexcolname);
  WRITE_NODE_FIELD(collation);
  WRITE_NODE_FIELD(opclass);
  WRITE_ENUM_FIELD(ordering, SortByDir);
  WRITE_ENUM_FIELD(nulls_ordering, SortByNulls);
}

static void
_outQuery(std::stringstream& str, const Query *node)
{
  WRITE_NODE_TYPE("QUERY");

  WRITE_ENUM_FIELD(commandType, CmdType);
  WRITE_ENUM_FIELD(querySource, QuerySource);
  /* we intentionally do not print the queryId field */
  WRITE_BOOL_FIELD(canSetTag);

  /*
   * Hack to work around missing outfuncs routines for a lot of the
   * utility-statement node types.  (The only one we actually *need* for
   * rules support is NotifyStmt.)  Someday we ought to support 'em all, but
   * for the meantime do this to avoid getting lots of warnings when running
   * with debug_print_parse on.
   */
  if (node->utilityStmt)
  {
    switch (nodeTag(node->utilityStmt))
    {
      case T_CreateStmt:
      case T_IndexStmt:
      case T_NotifyStmt:
      case T_DeclareCursorStmt:
        WRITE_NODE_FIELD(utilityStmt);
        break;
      default:
        appendStringInfoString(str, " :utilityStmt ?");
        break;
    }
  }
  else
    appendStringInfoString(str, " :utilityStmt <>");

  WRITE_INT_FIELD(resultRelation);
  WRITE_BOOL_FIELD(hasAggs);
  WRITE_BOOL_FIELD(hasWindowFuncs);
  WRITE_BOOL_FIELD(hasSubLinks);
  WRITE_BOOL_FIELD(hasDistinctOn);
  WRITE_BOOL_FIELD(hasRecursive);
  WRITE_BOOL_FIELD(hasModifyingCTE);
  WRITE_BOOL_FIELD(hasForUpdate);
  WRITE_BOOL_FIELD(hasRowSecurity);
  WRITE_NODE_FIELD(cteList);
  WRITE_NODE_FIELD(rtable);
  WRITE_NODE_FIELD(jointree);
  WRITE_NODE_FIELD(targetList);
  WRITE_NODE_FIELD(withCheckOptions);
  WRITE_NODE_FIELD(onConflict);
  WRITE_NODE_FIELD(returningList);
  WRITE_NODE_FIELD(groupClause);
  WRITE_NODE_FIELD(groupingSets);
  WRITE_NODE_FIELD(havingQual);
  WRITE_NODE_FIELD(windowClause);
  WRITE_NODE_FIELD(distinctClause);
  WRITE_NODE_FIELD(sortClause);
  WRITE_NODE_FIELD(limitOffset);
  WRITE_NODE_FIELD(limitCount);
  WRITE_NODE_FIELD(rowMarks);
  WRITE_NODE_FIELD(setOperations);
  WRITE_NODE_FIELD(constraintDeps);
}

static void
_outWithCheckOption(std::stringstream& str, const WithCheckOption *node)
{
  WRITE_NODE_TYPE("WITHCHECKOPTION");

  WRITE_ENUM_FIELD(kind, WCOKind);
  WRITE_STRING_FIELD(relname);
  WRITE_NODE_FIELD(qual);
  WRITE_BOOL_FIELD(cascaded);
}

static void
_outSortGroupClause(std::stringstream& str, const SortGroupClause *node)
{
  WRITE_NODE_TYPE("SORTGROUPCLAUSE");

  WRITE_UINT_FIELD(tleSortGroupRef);
  WRITE_OID_FIELD(eqop);
  WRITE_OID_FIELD(sortop);
  WRITE_BOOL_FIELD(nulls_first);
  WRITE_BOOL_FIELD(hashable);
}

static void
_outGroupingSet(std::stringstream& str, const GroupingSet *node)
{
  WRITE_NODE_TYPE("GROUPINGSET");

  WRITE_ENUM_FIELD(kind, GroupingSetKind);
  WRITE_NODE_FIELD(content);
  WRITE_LOCATION_FIELD(location);
}

static void
_outWindowClause(std::stringstream& str, const WindowClause *node)
{
  WRITE_NODE_TYPE("WINDOWCLAUSE");

  WRITE_STRING_FIELD(name);
  WRITE_STRING_FIELD(refname);
  WRITE_NODE_FIELD(partitionClause);
  WRITE_NODE_FIELD(orderClause);
  WRITE_INT_FIELD(frameOptions);
  WRITE_NODE_FIELD(startOffset);
  WRITE_NODE_FIELD(endOffset);
  WRITE_UINT_FIELD(winref);
  WRITE_BOOL_FIELD(copiedOrder);
}

static void
_outRowMarkClause(std::stringstream& str, const RowMarkClause *node)
{
  WRITE_NODE_TYPE("ROWMARKCLAUSE");

  WRITE_UINT_FIELD(rti);
  WRITE_ENUM_FIELD(strength, LockClauseStrength);
  WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
  WRITE_BOOL_FIELD(pushedDown);
}

static void
_outWithClause(std::stringstream& str, const WithClause *node)
{
  WRITE_NODE_TYPE("WITHCLAUSE");

  WRITE_NODE_FIELD(ctes);
  WRITE_BOOL_FIELD(recursive);
  WRITE_LOCATION_FIELD(location);
}

static void
_outCommonTableExpr(std::stringstream& str, const CommonTableExpr *node)
{
  WRITE_NODE_TYPE("COMMONTABLEEXPR");

  WRITE_STRING_FIELD(ctename);
  WRITE_NODE_FIELD(aliascolnames);
  WRITE_NODE_FIELD(ctequery);
  WRITE_LOCATION_FIELD(location);
  WRITE_BOOL_FIELD(cterecursive);
  WRITE_INT_FIELD(cterefcount);
  WRITE_NODE_FIELD(ctecolnames);
  WRITE_NODE_FIELD(ctecoltypes);
  WRITE_NODE_FIELD(ctecoltypmods);
  WRITE_NODE_FIELD(ctecolcollations);
}

static void
_outRangeTableSample(std::stringstream& str, const RangeTableSample *node)
{
  WRITE_NODE_TYPE("RANGETABLESAMPLE");

  WRITE_NODE_FIELD(relation);
  WRITE_STRING_FIELD(method);
  WRITE_NODE_FIELD(repeatable);
  WRITE_NODE_FIELD(args);
}

static void
_outTableSampleClause(std::stringstream& str, const TableSampleClause *node)
{
  WRITE_NODE_TYPE("TABLESAMPLECLAUSE");

  WRITE_OID_FIELD(tsmid);
  WRITE_BOOL_FIELD(tsmseqscan);
  WRITE_BOOL_FIELD(tsmpagemode);
  WRITE_OID_FIELD(tsminit);
  WRITE_OID_FIELD(tsmnextblock);
  WRITE_OID_FIELD(tsmnexttuple);
  WRITE_OID_FIELD(tsmexaminetuple);
  WRITE_OID_FIELD(tsmend);
  WRITE_OID_FIELD(tsmreset);
  WRITE_OID_FIELD(tsmcost);
  WRITE_NODE_FIELD(repeatable);
  WRITE_NODE_FIELD(args);
}

static void
_outSetOperationStmt(std::stringstream& str, const SetOperationStmt *node)
{
  WRITE_NODE_TYPE("SETOPERATIONSTMT");

  WRITE_ENUM_FIELD(op, SetOperation);
  WRITE_BOOL_FIELD(all);
  WRITE_NODE_FIELD(larg);
  WRITE_NODE_FIELD(rarg);
  WRITE_NODE_FIELD(colTypes);
  WRITE_NODE_FIELD(colTypmods);
  WRITE_NODE_FIELD(colCollations);
  WRITE_NODE_FIELD(groupClauses);
}

static void
_outRangeTblEntry(std::stringstream& str, const RangeTblEntry *node)
{
  WRITE_NODE_TYPE("RTE");

  /* put alias + eref first to make dump more legible */
  WRITE_NODE_FIELD(alias);
  WRITE_NODE_FIELD(eref);
  WRITE_ENUM_FIELD(rtekind, RTEKind);

  switch (node->rtekind)
  {
    case RTE_RELATION:
      WRITE_OID_FIELD(relid);
      WRITE_CHAR_FIELD(relkind);
      WRITE_NODE_FIELD(tablesample);
      break;
    case RTE_SUBQUERY:
      WRITE_NODE_FIELD(subquery);
      WRITE_BOOL_FIELD(security_barrier);
      break;
    case RTE_JOIN:
      WRITE_ENUM_FIELD(jointype, JoinType);
      WRITE_NODE_FIELD(joinaliasvars);
      break;
    case RTE_FUNCTION:
      WRITE_NODE_FIELD(functions);
      WRITE_BOOL_FIELD(funcordinality);
      break;
    case RTE_VALUES:
      WRITE_NODE_FIELD(values_lists);
      WRITE_NODE_FIELD(values_collations);
      break;
    case RTE_CTE:
      WRITE_STRING_FIELD(ctename);
      WRITE_UINT_FIELD(ctelevelsup);
      WRITE_BOOL_FIELD(self_reference);
      WRITE_NODE_FIELD(ctecoltypes);
      WRITE_NODE_FIELD(ctecoltypmods);
      WRITE_NODE_FIELD(ctecolcollations);
      break;
    default:
      LOG_ERROR("unrecognized RTE kind: %d", (int) node->rtekind);
      break;
  }

  WRITE_BOOL_FIELD(lateral);
  WRITE_BOOL_FIELD(inh);
  WRITE_BOOL_FIELD(inFromCl);
  WRITE_UINT_FIELD(requiredPerms);
  WRITE_OID_FIELD(checkAsUser);
  WRITE_BITMAPSET_FIELD(selectedCols);
  WRITE_BITMAPSET_FIELD(insertedCols);
  WRITE_BITMAPSET_FIELD(updatedCols);
  WRITE_NODE_FIELD(securityQuals);
}

static void
_outRangeTblFunction(std::stringstream& str, const RangeTblFunction *node)
{
  WRITE_NODE_TYPE("RANGETBLFUNCTION");

  WRITE_NODE_FIELD(funcexpr);
  WRITE_INT_FIELD(funccolcount);
  WRITE_NODE_FIELD(funccolnames);
  WRITE_NODE_FIELD(funccoltypes);
  WRITE_NODE_FIELD(funccoltypmods);
  WRITE_NODE_FIELD(funccolcollations);
  WRITE_BITMAPSET_FIELD(funcparams);
}

static void
_outAExpr(std::stringstream& str, const A_Expr *node)
{
  WRITE_NODE_TYPE("AEXPR");

  switch (node->kind)
  {
    case AEXPR_OP:
      appendStringInfoChar(str, ' ');
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_OP_ANY:
      appendStringInfoChar(str, ' ');
      WRITE_NODE_FIELD(name);
      appendStringInfoString(str, " ANY ");
      break;
    case AEXPR_OP_ALL:
      appendStringInfoChar(str, ' ');
      WRITE_NODE_FIELD(name);
      appendStringInfoString(str, " ALL ");
      break;
    case AEXPR_DISTINCT:
      appendStringInfoString(str, " DISTINCT ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_NULLIF:
      appendStringInfoString(str, " NULLIF ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_OF:
      appendStringInfoString(str, " OF ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_IN:
      appendStringInfoString(str, " IN ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_LIKE:
      appendStringInfoString(str, " LIKE ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_ILIKE:
      appendStringInfoString(str, " ILIKE ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_SIMILAR:
      appendStringInfoString(str, " SIMILAR ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_BETWEEN:
      appendStringInfoString(str, " BETWEEN ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_NOT_BETWEEN:
      appendStringInfoString(str, " NOT_BETWEEN ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_BETWEEN_SYM:
      appendStringInfoString(str, " BETWEEN_SYM ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_NOT_BETWEEN_SYM:
      appendStringInfoString(str, " NOT_BETWEEN_SYM ");
      WRITE_NODE_FIELD(name);
      break;
    case AEXPR_PAREN:
      appendStringInfoString(str, " PAREN");
      break;
    default:
      appendStringInfoString(str, " ??");
      break;
  }

  WRITE_NODE_FIELD(lexpr);
  WRITE_NODE_FIELD(rexpr);
  WRITE_LOCATION_FIELD(location);
}

static void
_outValue(std::stringstream& str, const Value *value)
{
  switch (value->type)
  {
    case T_Integer:
      appendStringInfo(str, "%ld", value->val.ival);
      break;
    case T_Float:

      /*
       * We assume the value is a valid numeric literal and so does not
       * need quoting.
       */
      appendStringInfoString(str, value->val.str);
      break;
    case T_String:

      /*
       * We use _outToken to provide escaping of the string's content,
       * but we don't want it to do anything with an empty string.
       */
      appendStringInfoChar(str, '"');
      if (value->val.str[0] != '\0')
        _outToken(str, value->val.str);
      appendStringInfoChar(str, '"');
      break;
    case T_BitString:
      /* internal representation already has leading 'b' */
      appendStringInfoString(str, value->val.str);
      break;
    case T_Null:
      /* this is seen only within A_Const, not in transformed trees */
      appendStringInfoString(str, "NULL");
      break;
    default:
      LOG_ERROR("unrecognized node type: %d", (int) value->type);
      break;
  }
}

static void
_outColumnRef(std::stringstream& str, const ColumnRef *node)
{
  WRITE_NODE_TYPE("COLUMNREF");

  WRITE_NODE_FIELD(fields);
  WRITE_LOCATION_FIELD(location);
}

static void
_outParamRef(std::stringstream& str, const ParamRef *node)
{
  WRITE_NODE_TYPE("PARAMREF");

  WRITE_INT_FIELD(number);
  WRITE_LOCATION_FIELD(location);
}

static void
_outAConst(std::stringstream& str, const A_Const *node)
{
  WRITE_NODE_TYPE("A_CONST");

  appendStringInfoString(str, " :val ");
  _outValue(str, &(node->val));
  WRITE_LOCATION_FIELD(location);
}

static void
_outA_Star(std::stringstream& str, const A_Star *node __attribute__((unused)))
{
  WRITE_NODE_TYPE("A_STAR");
}

static void
_outA_Indices(std::stringstream& str, const A_Indices *node)
{
  WRITE_NODE_TYPE("A_INDICES");

  WRITE_NODE_FIELD(lidx);
  WRITE_NODE_FIELD(uidx);
}

static void
_outA_Indirection(std::stringstream& str, const A_Indirection *node)
{
  WRITE_NODE_TYPE("A_INDIRECTION");

  WRITE_NODE_FIELD(arg);
  WRITE_NODE_FIELD(indirection);
}

static void
_outA_ArrayExpr(std::stringstream& str, const A_ArrayExpr *node)
{
  WRITE_NODE_TYPE("A_ARRAYEXPR");

  WRITE_NODE_FIELD(elements);
  WRITE_LOCATION_FIELD(location);
}

static void
_outResTarget(std::stringstream& str, const ResTarget *node)
{
  WRITE_NODE_TYPE("RESTARGET");

  WRITE_STRING_FIELD(name);
  WRITE_NODE_FIELD(indirection);
  WRITE_NODE_FIELD(val);
  WRITE_LOCATION_FIELD(location);
}

static void
_outMultiAssignRef(std::stringstream& str, const MultiAssignRef *node)
{
  WRITE_NODE_TYPE("MULTIASSIGNREF");

  WRITE_NODE_FIELD(source);
  WRITE_INT_FIELD(colno);
  WRITE_INT_FIELD(ncolumns);
}

static void
_outSortBy(std::stringstream& str, const SortBy *node)
{
  WRITE_NODE_TYPE("SORTBY");

  WRITE_NODE_FIELD(node);
  WRITE_ENUM_FIELD(sortby_dir, SortByDir);
  WRITE_ENUM_FIELD(sortby_nulls, SortByNulls);
  WRITE_NODE_FIELD(useOp);
  WRITE_LOCATION_FIELD(location);
}

static void
_outWindowDef(std::stringstream& str, const WindowDef *node)
{
  WRITE_NODE_TYPE("WINDOWDEF");

  WRITE_STRING_FIELD(name);
  WRITE_STRING_FIELD(refname);
  WRITE_NODE_FIELD(partitionClause);
  WRITE_NODE_FIELD(orderClause);
  WRITE_INT_FIELD(frameOptions);
  WRITE_NODE_FIELD(startOffset);
  WRITE_NODE_FIELD(endOffset);
  WRITE_LOCATION_FIELD(location);
}

static void
_outRangeSubselect(std::stringstream& str, const RangeSubselect *node)
{
  WRITE_NODE_TYPE("RANGESUBSELECT");

  WRITE_BOOL_FIELD(lateral);
  WRITE_NODE_FIELD(subquery);
  WRITE_NODE_FIELD(alias);
}

static void
_outRangeFunction(std::stringstream& str, const RangeFunction *node)
{
  WRITE_NODE_TYPE("RANGEFUNCTION");

  WRITE_BOOL_FIELD(lateral);
  WRITE_BOOL_FIELD(ordinality);
  WRITE_BOOL_FIELD(is_rowsfrom);
  WRITE_NODE_FIELD(functions);
  WRITE_NODE_FIELD(alias);
  WRITE_NODE_FIELD(coldeflist);
}

static void
_outConstraint(std::stringstream& str, const Constraint *node)
{
  WRITE_NODE_TYPE("CONSTRAINT");

  WRITE_STRING_FIELD(conname);
  WRITE_BOOL_FIELD(deferrable);
  WRITE_BOOL_FIELD(initdeferred);
  WRITE_LOCATION_FIELD(location);

  appendStringInfoString(str, " :contype ");
  switch (node->contype)
  {
    case CONSTR_NULL:
      appendStringInfoString(str, "NULL");
      break;

    case CONSTR_NOTNULL:
      appendStringInfoString(str, "NOT_NULL");
      break;

    case CONSTR_DEFAULT:
      appendStringInfoString(str, "DEFAULT");
      WRITE_NODE_FIELD(raw_expr);
      WRITE_STRING_FIELD(cooked_expr);
      break;

    case CONSTR_CHECK:
      appendStringInfoString(str, "CHECK");
      WRITE_BOOL_FIELD(is_no_inherit);
      WRITE_NODE_FIELD(raw_expr);
      WRITE_STRING_FIELD(cooked_expr);
      break;

    case CONSTR_PRIMARY:
      appendStringInfoString(str, "PRIMARY_KEY");
      WRITE_NODE_FIELD(keys);
      WRITE_NODE_FIELD(options);
      WRITE_STRING_FIELD(indexname);
      WRITE_STRING_FIELD(indexspace);
      /* access_method and where_clause not currently used */
      break;

    case CONSTR_UNIQUE:
      appendStringInfoString(str, "UNIQUE");
      WRITE_NODE_FIELD(keys);
      WRITE_NODE_FIELD(options);
      WRITE_STRING_FIELD(indexname);
      WRITE_STRING_FIELD(indexspace);
      /* access_method and where_clause not currently used */
      break;

    case CONSTR_EXCLUSION:
      appendStringInfoString(str, "EXCLUSION");
      WRITE_NODE_FIELD(exclusions);
      WRITE_NODE_FIELD(options);
      WRITE_STRING_FIELD(indexname);
      WRITE_STRING_FIELD(indexspace);
      WRITE_STRING_FIELD(access_method);
      WRITE_NODE_FIELD(where_clause);
      break;

    case CONSTR_FOREIGN:
      appendStringInfoString(str, "FOREIGN_KEY");
      WRITE_NODE_FIELD(pktable);
      WRITE_NODE_FIELD(fk_attrs);
      WRITE_NODE_FIELD(pk_attrs);
      WRITE_CHAR_FIELD(fk_matchtype);
      WRITE_CHAR_FIELD(fk_upd_action);
      WRITE_CHAR_FIELD(fk_del_action);
      WRITE_NODE_FIELD(old_conpfeqop);
      WRITE_OID_FIELD(old_pktable_oid);
      WRITE_BOOL_FIELD(skip_validation);
      WRITE_BOOL_FIELD(initially_valid);
      break;

    case CONSTR_ATTR_DEFERRABLE:
      appendStringInfoString(str, "ATTR_DEFERRABLE");
      break;

    case CONSTR_ATTR_NOT_DEFERRABLE:
      appendStringInfoString(str, "ATTR_NOT_DEFERRABLE");
      break;

    case CONSTR_ATTR_DEFERRED:
      appendStringInfoString(str, "ATTR_DEFERRED");
      break;

    case CONSTR_ATTR_IMMEDIATE:
      appendStringInfoString(str, "ATTR_IMMEDIATE");
      break;

    default:
      appendStringInfo(str, "<unrecognized_constraint %d>",
                       (int) node->contype);
      break;
  }
}


/*
 * _outNode -
 *	  converts a Node into ascii string and append it to 'str'
 */
static void
_outNode(std::stringstream& str, const void *obj)
{
  if (obj == NULL)
    appendStringInfoString(str, "<>");
  else if (IsA(obj, List) ||IsA(obj, IntList) || IsA(obj, OidList))
    _outList(str, (const List *) obj);
  else if (IsA(obj, Integer) ||
      IsA(obj, Float) ||
      IsA(obj, String) ||
      IsA(obj, BitString))
  {
    /* nodeRead does not want to see { } around these! */
    _outValue(str, (const Value *) obj);
  }
  else
  {
    appendStringInfoChar(str, '{');
    switch (nodeTag(obj))
    {
      case T_PlannedStmt:
        _outPlannedStmt(str, (const PlannedStmt *) obj);
        break;
      case T_Plan:
        _outPlan(str, (const Plan *) obj);
        break;
      case T_Result:
        _outResult(str, (const Result *) obj);
        break;
      case T_ModifyTable:
        _outModifyTable(str, (const ModifyTable *) obj);
        break;
      case T_Append:
        _outAppend(str, (const Append *) obj);
        break;
      case T_MergeAppend:
        _outMergeAppend(str, (const MergeAppend *) obj);
        break;
      case T_RecursiveUnion:
        _outRecursiveUnion(str, (const RecursiveUnion *) obj);
        break;
      case T_BitmapAnd:
        _outBitmapAnd(str, (const BitmapAnd *) obj);
        break;
      case T_BitmapOr:
        _outBitmapOr(str, (const BitmapOr *) obj);
        break;
      case T_Scan:
        _outScan(str, (const Scan *) obj);
        break;
      case T_SeqScan:
        _outSeqScan(str, (const SeqScan *) obj);
        break;
      case T_IndexScan:
        _outIndexScan(str, (const IndexScan *) obj);
        break;
      case T_IndexOnlyScan:
        _outIndexOnlyScan(str, (const IndexOnlyScan *) obj);
        break;
      case T_BitmapIndexScan:
        _outBitmapIndexScan(str, (const BitmapIndexScan *) obj);
        break;
      case T_BitmapHeapScan:
        _outBitmapHeapScan(str, (const BitmapHeapScan *) obj);
        break;
      case T_TidScan:
        _outTidScan(str, (const TidScan *) obj);
        break;
      case T_SubqueryScan:
        _outSubqueryScan(str, (const SubqueryScan *) obj);
        break;
      case T_FunctionScan:
        _outFunctionScan(str, (const FunctionScan *) obj);
        break;
      case T_ValuesScan:
        _outValuesScan(str, (const ValuesScan *) obj);
        break;
      case T_CteScan:
        _outCteScan(str, (const CteScan *) obj);
        break;
      case T_WorkTableScan:
        _outWorkTableScan(str, (const WorkTableScan *) obj);
        break;
      case T_ForeignScan:
        _outForeignScan(str, (const ForeignScan *) obj);
        break;
      case T_CustomScan:
        _outCustomScan(str, (const CustomScan *) obj);
        break;
      case T_SampleScan:
        _outSampleScan(str, (const SampleScan *) obj);
        break;
      case T_Join:
        _outJoin(str, (const Join *) obj);
        break;
      case T_NestLoop:
        _outNestLoop(str, (const NestLoop *) obj);
        break;
      case T_MergeJoin:
        _outMergeJoin(str, (const MergeJoin *) obj);
        break;
      case T_HashJoin:
        _outHashJoin(str, (const HashJoin *) obj);
        break;
      case T_Agg:
        _outAgg(str, (const Agg *) obj);
        break;
      case T_WindowAgg:
        _outWindowAgg(str, (const WindowAgg *) obj);
        break;
      case T_Group:
        _outGroup(str, (const Group *) obj);
        break;
      case T_Material:
        _outMaterial(str, (const Material *) obj);
        break;
      case T_Sort:
        _outSort(str, (const Sort *) obj);
        break;
      case T_Unique:
        _outUnique(str, (const Unique *) obj);
        break;
      case T_Hash:
        _outHash(str, (const Hash *) obj);
        break;
      case T_SetOp:
        _outSetOp(str, (const SetOp *) obj);
        break;
      case T_LockRows:
        _outLockRows(str, (const LockRows *) obj);
        break;
      case T_Limit:
        _outLimit(str, (const Limit *) obj);
        break;
      case T_NestLoopParam:
        _outNestLoopParam(str, (const NestLoopParam *) obj);
        break;
      case T_PlanRowMark:
        _outPlanRowMark(str, (const PlanRowMark *) obj);
        break;
      case T_PlanInvalItem:
        _outPlanInvalItem(str, (const PlanInvalItem *) obj);
        break;
      case T_Alias:
        _outAlias(str, (const Alias *) obj);
        break;
      case T_RangeVar:
        _outRangeVar(str, (const RangeVar *) obj);
        break;
      case T_IntoClause:
        _outIntoClause(str, (const IntoClause *) obj);
        break;
      case T_Var:
        _outVar(str, (const Var *) obj);
        break;
      case T_Const:
        _outConst(str, (const Const *) obj);
        break;
      case T_Param:
        _outParam(str, (const Param *) obj);
        break;
      case T_Aggref:
        _outAggref(str, (const Aggref *) obj);
        break;
      case T_GroupingFunc:
        _outGroupingFunc(str, (const GroupingFunc *) obj);
        break;
      case T_WindowFunc:
        _outWindowFunc(str, (const WindowFunc *) obj);
        break;
      case T_ArrayRef:
        _outArrayRef(str, (const ArrayRef *) obj);
        break;
      case T_FuncExpr:
        _outFuncExpr(str, (const FuncExpr *) obj);
        break;
      case T_NamedArgExpr:
        _outNamedArgExpr(str, (const NamedArgExpr *) obj);
        break;
      case T_OpExpr:
        _outOpExpr(str, (const OpExpr *) obj);
        break;
      case T_DistinctExpr:
        _outDistinctExpr(str, (const DistinctExpr *) obj);
        break;
      case T_NullIfExpr:
        _outNullIfExpr(str, (const NullIfExpr *) obj);
        break;
      case T_ScalarArrayOpExpr:
        _outScalarArrayOpExpr(str, (const ScalarArrayOpExpr *) obj);
        break;
      case T_BoolExpr:
        _outBoolExpr(str, (const BoolExpr *) obj);
        break;
      case T_SubLink:
        _outSubLink(str, (const SubLink *) obj);
        break;
      case T_SubPlan:
        _outSubPlan(str, (const SubPlan *) obj);
        break;
      case T_AlternativeSubPlan:
        _outAlternativeSubPlan(str, (const AlternativeSubPlan *) obj);
        break;
      case T_FieldSelect:
        _outFieldSelect(str, (const FieldSelect *) obj);
        break;
      case T_FieldStore:
        _outFieldStore(str, (const FieldStore *) obj);
        break;
      case T_RelabelType:
        _outRelabelType(str, (const RelabelType *) obj);
        break;
      case T_CoerceViaIO:
        _outCoerceViaIO(str, (const CoerceViaIO *) obj);
        break;
      case T_ArrayCoerceExpr:
        _outArrayCoerceExpr(str, (const ArrayCoerceExpr *) obj);
        break;
      case T_ConvertRowtypeExpr:
        _outConvertRowtypeExpr(str, (const ConvertRowtypeExpr *) obj);
        break;
      case T_CollateExpr:
        _outCollateExpr(str, (const CollateExpr *) obj);
        break;
      case T_CaseExpr:
        _outCaseExpr(str, (const CaseExpr *) obj);
        break;
      case T_CaseWhen:
        _outCaseWhen(str, (const CaseWhen *) obj);
        break;
      case T_CaseTestExpr:
        _outCaseTestExpr(str, (const CaseTestExpr *) obj);
        break;
      case T_ArrayExpr:
        _outArrayExpr(str, (const ArrayExpr *) obj);
        break;
      case T_RowExpr:
        _outRowExpr(str, (const RowExpr *) obj);
        break;
      case T_RowCompareExpr:
        _outRowCompareExpr(str, (const RowCompareExpr *) obj);
        break;
      case T_CoalesceExpr:
        _outCoalesceExpr(str, (const CoalesceExpr *) obj);
        break;
      case T_MinMaxExpr:
        _outMinMaxExpr(str, (const MinMaxExpr *) obj);
        break;
      case T_XmlExpr:
        _outXmlExpr(str, (const XmlExpr *) obj);
        break;
      case T_NullTest:
        _outNullTest(str, (const NullTest *) obj);
        break;
      case T_BooleanTest:
        _outBooleanTest(str, (const BooleanTest *) obj);
        break;
      case T_CoerceToDomain:
        _outCoerceToDomain(str, (const CoerceToDomain *) obj);
        break;
      case T_CoerceToDomainValue:
        _outCoerceToDomainValue(str, (const CoerceToDomainValue *) obj);
        break;
      case T_SetToDefault:
        _outSetToDefault(str, (const SetToDefault *) obj);
        break;
      case T_CurrentOfExpr:
        _outCurrentOfExpr(str, (const CurrentOfExpr *) obj);
        break;
      case T_InferenceElem:
        _outInferenceElem(str, (const InferenceElem *) obj);
        break;
      case T_TargetEntry:
        _outTargetEntry(str, (const TargetEntry *) obj);
        break;
      case T_RangeTblRef:
        _outRangeTblRef(str, (const RangeTblRef *) obj);
        break;
      case T_JoinExpr:
        _outJoinExpr(str, (const JoinExpr *) obj);
        break;
      case T_FromExpr:
        _outFromExpr(str, (const FromExpr *) obj);
        break;
      case T_OnConflictExpr:
        _outOnConflictExpr(str, (const OnConflictExpr *) obj);
        break;
      case T_Path:
        _outPath(str, (const Path *) obj);
        break;
      case T_IndexPath:
        _outIndexPath(str, (const IndexPath *) obj);
        break;
      case T_BitmapHeapPath:
        _outBitmapHeapPath(str, (const BitmapHeapPath *) obj);
        break;
      case T_BitmapAndPath:
        _outBitmapAndPath(str, (const BitmapAndPath *) obj);
        break;
      case T_BitmapOrPath:
        _outBitmapOrPath(str, (const BitmapOrPath *) obj);
        break;
      case T_TidPath:
        _outTidPath(str, (const TidPath *) obj);
        break;
      case T_ForeignPath:
        _outForeignPath(str, (const ForeignPath *) obj);
        break;
      case T_CustomPath:
        _outCustomPath(str, (const CustomPath *) obj);
        break;
      case T_AppendPath:
        _outAppendPath(str, (const AppendPath *) obj);
        break;
      case T_MergeAppendPath:
        _outMergeAppendPath(str, (const MergeAppendPath *) obj);
        break;
      case T_ResultPath:
        _outResultPath(str, (const ResultPath *) obj);
        break;
      case T_MaterialPath:
        _outMaterialPath(str, (const MaterialPath *) obj);
        break;
      case T_UniquePath:
        _outUniquePath(str, (const UniquePath *) obj);
        break;
      case T_NestPath:
        _outNestPath(str, (const NestPath *) obj);
        break;
      case T_MergePath:
        _outMergePath(str, (const MergePath *) obj);
        break;
      case T_HashPath:
        _outHashPath(str, (const HashPath *) obj);
        break;
      case T_PlannerGlobal:
        _outPlannerGlobal(str, (const PlannerGlobal *) obj);
        break;
      case T_PlannerInfo:
        _outPlannerInfo(str, (const PlannerInfo *) obj);
        break;
      case T_RelOptInfo:
        _outRelOptInfo(str, (const RelOptInfo *) obj);
        break;
      case T_IndexOptInfo:
        _outIndexOptInfo(str, (const IndexOptInfo *) obj);
        break;
      case T_EquivalenceClass:
        _outEquivalenceClass(str, (const EquivalenceClass *) obj);
        break;
      case T_EquivalenceMember:
        _outEquivalenceMember(str, (const EquivalenceMember *) obj);
        break;
      case T_PathKey:
        _outPathKey(str, (const PathKey *) obj);
        break;
      case T_ParamPathInfo:
        _outParamPathInfo(str, (const ParamPathInfo *) obj);
        break;
      case T_RestrictInfo:
        _outRestrictInfo(str, (const RestrictInfo *) obj);
        break;
      case T_PlaceHolderVar:
        _outPlaceHolderVar(str, (const PlaceHolderVar *) obj);
        break;
      case T_SpecialJoinInfo:
        _outSpecialJoinInfo(str, (const SpecialJoinInfo *) obj);
        break;
      case T_LateralJoinInfo:
        _outLateralJoinInfo(str, (const LateralJoinInfo *) obj);
        break;
      case T_AppendRelInfo:
        _outAppendRelInfo(str, (const AppendRelInfo *) obj);
        break;
      case T_PlaceHolderInfo:
        _outPlaceHolderInfo(str, (const PlaceHolderInfo *) obj);
        break;
      case T_MinMaxAggInfo:
        _outMinMaxAggInfo(str, (const MinMaxAggInfo *) obj);
        break;
      case T_PlannerParamItem:
        _outPlannerParamItem(str, (const PlannerParamItem *) obj);
        break;

      case T_CreateStmt:
        _outCreateStmt(str, (const CreateStmt *) obj);
        break;
      case T_CreateForeignTableStmt:
        _outCreateForeignTableStmt(str, (const CreateForeignTableStmt *) obj);
        break;
      case T_ImportForeignSchemaStmt:
        _outImportForeignSchemaStmt(str, (const ImportForeignSchemaStmt *) obj);
        break;
      case T_IndexStmt:
        _outIndexStmt(str, (const IndexStmt *) obj);
        break;
      case T_NotifyStmt:
        _outNotifyStmt(str, (const NotifyStmt *) obj);
        break;
      case T_DeclareCursorStmt:
        _outDeclareCursorStmt(str, (const DeclareCursorStmt *) obj);
        break;
      case T_SelectStmt:
        _outSelectStmt(str, (const SelectStmt *) obj);
        break;
      case T_ColumnDef:
        _outColumnDef(str, (const ColumnDef *) obj);
        break;
      case T_TypeName:
        _outTypeName(str, (const TypeName *) obj);
        break;
      case T_TypeCast:
        _outTypeCast(str, (const TypeCast *) obj);
        break;
      case T_CollateClause:
        _outCollateClause(str, (const CollateClause *) obj);
        break;
      case T_IndexElem:
        _outIndexElem(str, (const IndexElem *) obj);
        break;
      case T_Query:
        _outQuery(str, (const Query *) obj);
        break;
      case T_WithCheckOption:
        _outWithCheckOption(str, (const WithCheckOption *) obj);
        break;
      case T_SortGroupClause:
        _outSortGroupClause(str, (const SortGroupClause *) obj);
        break;
      case T_GroupingSet:
        _outGroupingSet(str, (const GroupingSet *) obj);
        break;
      case T_WindowClause:
        _outWindowClause(str, (const WindowClause *) obj);
        break;
      case T_RowMarkClause:
        _outRowMarkClause(str, (const RowMarkClause *) obj);
        break;
      case T_WithClause:
        _outWithClause(str, (const WithClause *) obj);
        break;
      case T_CommonTableExpr:
        _outCommonTableExpr(str, (const CommonTableExpr *) obj);
        break;
      case T_RangeTableSample:
        _outRangeTableSample(str, (const RangeTableSample *) obj);
        break;
      case T_TableSampleClause:
        _outTableSampleClause(str, (const TableSampleClause *) obj);
        break;
      case T_SetOperationStmt:
        _outSetOperationStmt(str, (const SetOperationStmt *) obj);
        break;
      case T_RangeTblEntry:
        _outRangeTblEntry(str, (const RangeTblEntry *) obj);
        break;
      case T_RangeTblFunction:
        _outRangeTblFunction(str, (const RangeTblFunction *) obj);
        break;
      case T_A_Expr:
        _outAExpr(str, (const A_Expr *) obj);
        break;
      case T_ColumnRef:
        _outColumnRef(str, (const ColumnRef *) obj);
        break;
      case T_ParamRef:
        _outParamRef(str, (const ParamRef *) obj);
        break;
      case T_A_Const:
        _outAConst(str, (const A_Const *) obj);
        break;
      case T_A_Star:
        _outA_Star(str, (const A_Star *) obj);
        break;
      case T_A_Indices:
        _outA_Indices(str, (const A_Indices *) obj);
        break;
      case T_A_Indirection:
        _outA_Indirection(str, (const A_Indirection *) obj);
        break;
      case T_A_ArrayExpr:
        _outA_ArrayExpr(str, (const A_ArrayExpr *) obj);
        break;
      case T_ResTarget:
        _outResTarget(str, (const ResTarget *) obj);
        break;
      case T_MultiAssignRef:
        _outMultiAssignRef(str, (const MultiAssignRef *) obj);
        break;
      case T_SortBy:
        _outSortBy(str, (const SortBy *) obj);
        break;
      case T_WindowDef:
        _outWindowDef(str, (const WindowDef *) obj);
        break;
      case T_RangeSubselect:
        _outRangeSubselect(str, (const RangeSubselect *) obj);
        break;
      case T_RangeFunction:
        _outRangeFunction(str, (const RangeFunction *) obj);
        break;
      case T_Constraint:
        _outConstraint(str, (const Constraint *) obj);
        break;
      case T_FuncCall:
        _outFuncCall(str, (const FuncCall *) obj);
        break;
      case T_DefElem:
        _outDefElem(str, (const DefElem *) obj);
        break;
      case T_TableLikeClause:
        _outTableLikeClause(str, (const TableLikeClause *) obj);
        break;
      case T_LockingClause:
        _outLockingClause(str, (const LockingClause *) obj);
        break;
      case T_XmlSerialize:
        _outXmlSerialize(str, (const XmlSerialize *) obj);
        break;

      default:

        /*
         * This should be an ERROR, but it's too useful to be able to
         * dump structures that _outNode only understands part of.
         */
        LOG_WARN("could not dump unrecognized node type: %d",
                 (int) nodeTag(obj));
        break;
    }
    appendStringInfoChar(str, '}');
  }
}

/*
 * nodeToString -
 *	   returns the ascii representation of the Node as a palloc'd string
 */
std::string
NodeToString(const void *obj)
{
  std::stringstream ss;

  /* see stringinfo.h for an explanation of this maneuver */
  _outNode(ss, obj);

  return ss.str();
}

} // namespace backend
} // namespace nstore
