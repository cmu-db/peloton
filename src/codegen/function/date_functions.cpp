#include "codegen/function/functions.h"
#include "codegen/proxy/builtin_function_proxy.h"
#include "codegen/type/sql_type.h"
#include <date/date.h>
#include <date/iso_week.h>

namespace peloton {
namespace codegen {
namespace function {

codegen::Value DateFunctions::Extract(CodeGen &codegen, CompilationContext &ctx,
                                      const std::vector<codegen::Value> &args) {
  llvm::Function *func = DateFunctionsProxy::Extract_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::DECIMAL),
                        ret_val, nullptr);
}

double DateFunctions::Extract_(
    UNUSED_ATTRIBUTE executor::ExecutorContext *executor_context,
    int32_t date_part, uint64_t timestamp) {
  if (timestamp == 0) {
    return 0.0;
  }

  uint32_t micro = timestamp % 1000000;
  timestamp /= 1000000;
  uint32_t hour_min_sec = timestamp % 100000;
  uint16_t sec = hour_min_sec % 60;
  hour_min_sec /= 60;
  uint16_t min = hour_min_sec % 60;
  hour_min_sec /= 60;
  uint16_t hour = hour_min_sec % 24;
  timestamp /= 100000;
  uint16_t year = timestamp % 10000;
  timestamp /= 10000;
  timestamp /= 27;  // skip time zone
  uint16_t day = timestamp % 32;
  timestamp /= 32;
  uint16_t month = timestamp;

  uint16_t millennium = (year - 1) / 1000 + 1;
  uint16_t century = (year - 1) / 100 + 1;
  uint16_t decade = year / 10;
  uint8_t quarter = (month - 1) / 3 + 1;

  double microsecond = sec * 1000000 + micro;
  double millisecond = sec * 1000 + micro / 1000.0;
  double second = sec + micro / 1000000.0;

  date::year_month_day ymd = date::year_month_day{
      date::year{year}, date::month{month}, date::day{day}};
  iso_week::year_weeknum_weekday yww = iso_week::year_weeknum_weekday{ymd};

  date::year_month_day year_begin =
      date::year_month_day{date::year{year}, date::month{1}, date::day{1}};
  date::days duration = date::sys_days{ymd} - date::sys_days{year_begin};

  uint16_t dow = ((unsigned) yww.weekday()) == 7 ? 0 : (unsigned) yww.weekday();
  uint16_t doy = duration.count() + 1;
  uint16_t week = (unsigned) yww.weeknum();

  double result;

  switch (static_cast<DatePartType>(date_part)) {
    case DatePartType::CENTURY: {
      result = century;
      break;
    }
    case DatePartType::DAY: {
      result = day;
      break;
    }
    case DatePartType::DECADE: {
      result = decade;
      break;
    }
    case DatePartType::DOW: {
      result = dow;
      break;
    }
    case DatePartType::DOY: {
      result = doy;
      break;
    }
    case DatePartType::HOUR: {
      result = hour;
      break;
    }
    case DatePartType::MICROSECOND: {
      result = microsecond;
      break;
    }
    case DatePartType::MILLENNIUM: {
      result = millennium;
      break;
    }
    case DatePartType::MILLISECOND: {
      result = millisecond;
      break;
    }
    case DatePartType::MINUTE: {
      result = min;
      break;
    }
    case DatePartType::MONTH: {
      result = month;
      break;
    }
    case DatePartType::QUARTER: {
      result = quarter;
      break;
    }
    case DatePartType::SECOND: {
      result = second;
      break;
    }
    case DatePartType::WEEK: {
      result = week;
      break;
    }
    case DatePartType::YEAR: {
      result = year;
      break;
    }
    default: {
      result = 0.0;
    }
  };

  return result;
}

}  // namespace function
}  // namespace expression
}  // namespace peloton
