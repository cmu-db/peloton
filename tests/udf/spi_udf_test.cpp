#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "utils/builtins.h"


#ifdef __cplusplus
extern "C" {
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

int execq(text *sql);

int
execq(text *sql)
{
  char *command;
  int ret;
  int proc;

  /* Convert given text object to a C string */
  command = text_to_cstring(sql);

  SPI_connect();

  ret = SPI_exec(command, 0);

  proc = SPI_processed;
  /*
   * If some rows were fetched, print them via elog(INFO).
   */
  if (ret > 0 && SPI_tuptable != NULL)
  {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    char buf[8192];

    int i, j;

    for (j = 0; j < proc; j++)
    {
      HeapTuple tuple = tuptable->vals[j];

      for (i = 1, buf[0] = 0; i <= tupdesc->natts; i++)
        snprintf(buf + strlen (buf), sizeof(buf) - strlen(buf), " %s%s",
                 SPI_getvalue(tuple, tupdesc, i),
                 (i == tupdesc->natts) ? " " : " |");
      elog(INFO, "%s", buf);
    }
  }

  SPI_finish();
  pfree(command);

  return (proc);
}


PG_FUNCTION_INFO_V1(item_sales_sum_c);
Datum
item_sales_sum_c(PG_FUNCTION_ARGS) {
  int ret;
  int proc;
  float8 result;
  double val;
  char * end;
  char command[512];

  int32 item_id = PG_GETARG_INT32(0);
  sprintf(command, "select item.i_price from order_line,item where order_line.ol_i_id = %d and order_line.ol_i_id = item.i_id", item_id);

  SPI_connect();

  ret = SPI_exec(command, 0);

  proc = SPI_processed;
  result = 0.0;

  if (ret > 0 && SPI_tuptable != NULL)
  {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    char buf[8192];
    int i, j;

    for (j = 0; j < proc; j++)
    {
      HeapTuple tuple = tuptable->vals[j];

      for (i = 1, buf[0] = 0; i <= tupdesc->natts; i++)
        val = strtod(SPI_getvalue(tuple, tupdesc, i), &end);
        result += val;
    }
  }

  SPI_finish();

  PG_RETURN_FLOAT8(result);
}


#ifdef __cplusplus
};
#endif
