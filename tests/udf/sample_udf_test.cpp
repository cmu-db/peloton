#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "utils/geo_decls.h"
#include "access/htup_details.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/lsyscache.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* by value */
PG_FUNCTION_INFO_V1(add_one);

Datum add_one(PG_FUNCTION_ARGS) {
  int32 arg = PG_GETARG_INT32(0);

  PG_RETURN_INT32(arg + 1);
}

/* by value */
PG_FUNCTION_INFO_V1(add);

Datum add(PG_FUNCTION_ARGS) {
  int32 arg0 = PG_GETARG_INT32(0);
  int32 arg1 = PG_GETARG_INT32(1);

  PG_RETURN_INT32(arg0 + arg1);
}

/* by value */
PG_FUNCTION_INFO_V1(minus);

Datum minus(PG_FUNCTION_ARGS) {
  int32 arg0 = PG_GETARG_INT32(0);
  int32 arg1 = PG_GETARG_INT32(1);

  PG_RETURN_INT32(arg0 - arg1);
}

/* by value */
PG_FUNCTION_INFO_V1(multiply);

Datum multiply(PG_FUNCTION_ARGS) {
  int32 arg0 = PG_GETARG_INT32(0);
  int32 arg1 = PG_GETARG_INT32(1);

  PG_RETURN_INT32(arg0 * arg1);
}

/* by value */
PG_FUNCTION_INFO_V1(divide);

Datum divide(PG_FUNCTION_ARGS) {
  int32 arg0 = PG_GETARG_INT32(0);
  int32 arg1 = PG_GETARG_INT32(1);

  PG_RETURN_INT32(arg0 / arg1);
}

/* by reference, fixed length */

PG_FUNCTION_INFO_V1(add_one_float8);

Datum add_one_float8(PG_FUNCTION_ARGS) {
  /* The macros for FLOAT8 hide its pass-by-reference nature. */
  float8 arg = PG_GETARG_FLOAT8(0);

  PG_RETURN_FLOAT8(arg + 1.0);
}

PG_FUNCTION_INFO_V1(makepoint);

Datum makepoint(PG_FUNCTION_ARGS) {
  /* Here, the pass-by-reference nature of Point is not hidden. */
  Point *pointx = PG_GETARG_POINT_P(0);
  Point *pointy = PG_GETARG_POINT_P(1);
  Point *new_point = (Point *)palloc(sizeof(Point));

  new_point->x = pointx->x;
  new_point->y = pointy->y;

  PG_RETURN_POINT_P(new_point);
}

/* by reference, variable length */

PG_FUNCTION_INFO_V1(copy_text);

Datum copy_text(PG_FUNCTION_ARGS) {
  text *t = PG_GETARG_TEXT_P(0);
  /*
   * VARSIZE is the total size of the struct in bytes.
   */
  text *new_t = (text *)palloc(VARSIZE(t));
  SET_VARSIZE(new_t, VARSIZE(t));
  /*
   * VARDATA is a pointer to the data region of the struct.
   */
  memcpy((void *)VARDATA(new_t), /* destination */
         (void *)VARDATA(t),     /* source */
         VARSIZE(t) - VARHDRSZ); /* how many bytes */
  PG_RETURN_TEXT_P(new_t);
}

PG_FUNCTION_INFO_V1(concat_text);

Datum concat_text(PG_FUNCTION_ARGS) {
  text *arg1 = PG_GETARG_TEXT_P(0);
  text *arg2 = PG_GETARG_TEXT_P(1);
  int32 new_text_size = VARSIZE(arg1) + VARSIZE(arg2) - VARHDRSZ;
  text *new_text = (text *)palloc(new_text_size);

  SET_VARSIZE(new_text, new_text_size);
  memcpy(VARDATA(new_text), VARDATA(arg1), VARSIZE(arg1) - VARHDRSZ);
  memcpy(VARDATA(new_text) + (VARSIZE(arg1) - VARHDRSZ), VARDATA(arg2),
         VARSIZE(arg2) - VARHDRSZ);
  PG_RETURN_TEXT_P(new_text);
}

PG_FUNCTION_INFO_V1(sum_columns);

Datum sum_columns(PG_FUNCTION_ARGS) {
  HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
  bool isnullx, isnully;
  int32 x, y;

  x = DatumGetInt32(GetAttributeByName(t, "x", &isnullx));
  y = DatumGetInt32(GetAttributeByName(t, "y", &isnully));
  if (isnullx || isnully) {
    PG_RETURN_INT32(0);
  } else {
    PG_RETURN_INT32(x + y);
  }
}

PG_FUNCTION_INFO_V1(calc_tax);

Datum calc_tax(PG_FUNCTION_ARGS) {
  float8 price = PG_GETARG_FLOAT8(0);
  float8 tax = 0.0;
  if (price < 10.0) {
    tax = 0.0;
  } else if (price < 50.0) {
    tax = 0.06 * (price - 10.0);
  } else {
    tax = 0.06 * (50.0 - 10.0) + 0.09 * (price - 50.0);
  }
  PG_RETURN_FLOAT8(tax);
}

PG_FUNCTION_INFO_V1(replace_vowel);

Datum replace_vowel(PG_FUNCTION_ARGS) {
  text *arg = PG_GETARG_TEXT_P(0);
  int len = VARSIZE(arg) - VARHDRSZ;
  for (int i = 0; i < len; i++) {
    if (arg->vl_dat[i] == 'a' || arg->vl_dat[i] == 'e' ||
        arg->vl_dat[i] == 'i' || arg->vl_dat[i] == 'o' ||
        arg->vl_dat[i] == 'u') {
      arg->vl_dat[i] = '*';
    }
  }
  PG_RETURN_TEXT_P(arg);
}

PG_FUNCTION_INFO_V1(integer_manipulate);

Datum integer_manipulate(PG_FUNCTION_ARGS) {
  int32 arg = PG_GETARG_INT32(0);
  PG_RETURN_INT32((arg * 9 + 999) / 5 - 100);
}

PG_FUNCTION_INFO_V1(fib_c);

Datum fib_c(PG_FUNCTION_ARGS) {
  int32 num = PG_GETARG_INT32(0);
  if (num < 1) {
    PG_RETURN_INT32(-1);
  } else if (num == 1 || num == 2) {
    PG_RETURN_INT32(1);
  } else {
    int i;
    int a = 1;
    int b = 1;
    int c = 0;
    for (i = 3; i <= num; i++) {
      c = a + b;
      a = b;
      b = c;
    }
    PG_RETURN_INT32(c);
  }
}

PG_FUNCTION_INFO_V1(countdown_c);

Datum countdown_c(PG_FUNCTION_ARGS) {
  int32 start = PG_GETARG_INT32(0);
  int n = 1 + (int)log10(start);
  int sum = 0;
  int ten_pow = 1;
  int i;
  for (i = 1; i < n; i++) {
    sum += 9 * ten_pow * i;
    ten_pow *= 10;
  }
  sum += n * (start + 1 - ten_pow);
  char *result = new char[sum + start * 2];

  int len_num1 = 0;
  int s;
  s = start;
  while (s != 0) {
    s /= 10;
    len_num1++;
  }
  char *num1 = new char[len_num1 + 2];
  sprintf(num1, "%d\r\n", start);
  strcpy(result, num1);

  int j;
  for (i = start - 1; i >= 1; i--) {
    int len_num = 0;
    s = i;
    j = s;
  }

  PG_RETURN_INT32(start);
}

#ifdef __cplusplus
};
#endif
