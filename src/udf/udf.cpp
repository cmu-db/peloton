#include <include/udf/udf.h>

namespace peloton {
namespace udf {

//TODO(Haoran): filling this thing
bool UDFHandle::compile() {
  return true;
}

//TODO(Haoran): filling this thing
int UDFHandle::execute(std::vector<UDFHandle::arg_value> values) {
  return values[0];
}

}  // namespace udf
}  // namespace peloton
