#include <sqlite3.h>
#include <string>
#include <vector>
#include "marshall.h"

namespace peloton {
namespace wire {

struct Portal {
  std::string portal_name;
  std::string prep_stmt_name;
  std::vector<wiredb::FieldInfoType> rowdesc;
  std::string query_string;
  std::string query_type;
  sqlite3_stmt *stmt;
	size_t colcount;
};

} // namespace wire
} //namespace peloton
