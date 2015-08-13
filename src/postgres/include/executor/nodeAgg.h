/*-------------------------------------------------------------------------
 *
 * nodeAgg.h
 *	  prototypes for nodeAgg.c
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeAgg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEAGG_H
#define NODEAGG_H

#include "nodes/execnodes.h"

extern AggState *ExecInitAgg(Agg *node, EState *estate, int eflags);
extern TupleTableSlot *ExecAgg(AggState *node);
extern void ExecEndAgg(AggState *node);
extern void ExecReScanAgg(AggState *node);

extern Size hash_agg_entry_size(int numAggs);

extern Datum aggregate_dummy(PG_FUNCTION_ARGS);


/*
 * NB: Moved from nodeAgg.c to here by Peloton.
 * AggStatePerAggData - per-aggregate working state for the Agg scan
 */
typedef struct AggStatePerAggData
{
  /*
   * These values are set up during ExecInitAgg() and do not change
   * thereafter:
   */

  /* Links to Aggref expr and state nodes this working state is for */
  AggrefExprState *aggrefstate;
  Aggref     *aggref;

  /*
   * Nominal number of arguments for aggregate function.  For plain aggs,
   * this excludes any ORDER BY expressions.  For ordered-set aggs, this
   * counts both the direct and aggregated (ORDER BY) arguments.
   */
  int     numArguments;

  /*
   * Number of aggregated input columns.  This includes ORDER BY expressions
   * in both the plain-agg and ordered-set cases.  Ordered-set direct args
   * are not counted, though.
   */
  int     numInputs;

  /*
   * Number of aggregated input columns to pass to the transfn.  This
   * includes the ORDER BY columns for ordered-set aggs, but not for plain
   * aggs.  (This doesn't count the transition state value!)
   */
  int     numTransInputs;

  /*
   * Number of arguments to pass to the finalfn.  This is always at least 1
   * (the transition state value) plus any ordered-set direct args. If the
   * finalfn wants extra args then we pass nulls corresponding to the
   * aggregated input columns.
   */
  int     numFinalArgs;

  /* Oids of transfer functions */
  Oid     transfn_oid;
  Oid     finalfn_oid;  /* may be InvalidOid */

  /*
   * fmgr lookup data for transfer functions --- only valid when
   * corresponding oid is not InvalidOid.  Note in particular that fn_strict
   * flags are kept here.
   */
  FmgrInfo  transfn;
  FmgrInfo  finalfn;

  /* Input collation derived for aggregate */
  Oid     aggCollation;

  /* number of sorting columns */
  int     numSortCols;

  /* number of sorting columns to consider in DISTINCT comparisons */
  /* (this is either zero or the same as numSortCols) */
  int     numDistinctCols;

  /* deconstructed sorting information (arrays of length numSortCols) */
  AttrNumber *sortColIdx;
  Oid      *sortOperators;
  Oid      *sortCollations;
  bool     *sortNullsFirst;

  /*
   * fmgr lookup data for input columns' equality operators --- only
   * set/used when aggregate has DISTINCT flag.  Note that these are in
   * order of sort column index, not parameter index.
   */
  FmgrInfo   *equalfns;   /* array of length numDistinctCols */

  /*
   * initial value from pg_aggregate entry
   */
  Datum   initValue;
  bool    initValueIsNull;

  /*
   * We need the len and byval info for the agg's input, result, and
   * transition data types in order to know how to copy/delete values.
   *
   * Note that the info for the input type is used only when handling
   * DISTINCT aggs with just one argument, so there is only one input type.
   */
  int16   inputtypeLen,
        resulttypeLen,
        transtypeLen;
  bool    inputtypeByVal,
        resulttypeByVal,
        transtypeByVal;

  /*
   * Stuff for evaluation of inputs.  We used to just use ExecEvalExpr, but
   * with the addition of ORDER BY we now need at least a slot for passing
   * data to the sort object, which requires a tupledesc, so we might as
   * well go whole hog and use ExecProject too.
   */
  TupleDesc evaldesc;   /* descriptor of input tuples */
  ProjectionInfo *evalproj; /* projection machinery */

  /*
   * Slots for holding the evaluated input arguments.  These are set up
   * during ExecInitAgg() and then used for each input row.
   */
  TupleTableSlot *evalslot; /* current input tuple */
  TupleTableSlot *uniqslot; /* used for multi-column DISTINCT */

  /*
   * These values are working state that is initialized at the start of an
   * input tuple group and updated for each input tuple.
   *
   * For a simple (non DISTINCT/ORDER BY) aggregate, we just feed the input
   * values straight to the transition function.  If it's DISTINCT or
   * requires ORDER BY, we pass the input values into a Tuplesort object;
   * then at completion of the input tuple group, we scan the sorted values,
   * eliminate duplicates if needed, and run the transition function on the
   * rest.
   *
   * We need a separate tuplesort for each grouping set.
   */

  Tuplesortstate **sortstates;  /* sort objects, if DISTINCT or ORDER BY */

  /*
   * This field is a pre-initialized FunctionCallInfo struct used for
   * calling this aggregate's transfn.  We save a few cycles per row by not
   * re-initializing the unchanging fields; which isn't much, but it seems
   * worth the extra space consumption.
   */
  FunctionCallInfoData transfn_fcinfo;
} AggStatePerAggData;

#endif   /* NODEAGG_H */
