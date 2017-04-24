#include<tuple>
#include<vector>
#include<string>

namespace peloton {
namespace udf {

class UDFHandle {
 public:
  using arg_type=int;
  using arg_value=int;
  using arg_tuple=std::tuple<std::string, arg_type>;

  // TODO: add argument list & return type

  // Return false if validation fails
  bool compile();

  // Return the executed value
  int execute(std::vector<arg_value>);

  UDFHandle(std::string func, std::vector<std::string> args_name,
      std::vector<arg_type> args_type, arg_type ret_type)
    :body_{func}, args_name_{args_name}, args_type_{args_type}, ret_type_{ret_type}
  {}

 private:
  std::string body_;
  std::vector<std::string> args_name_;
  std::vector<arg_type> args_type_{};
  std::vector<arg_value> args_value_;
  arg_type ret_type_;
};

}  // namespace udf
}  // namespace peloton
