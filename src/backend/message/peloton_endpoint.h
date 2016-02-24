/**
*
* example: echo endpoint only
*
*/
#pragma once

namespace peloton {
namespace message {

#define PELOTON_ENDPOINT_ADDR "tcp://127.0.0.1:9999"
#define PELOTON_ENDPOINT_FILE "ipc:///tmp/echo.sock"

}  // namespace message
}  // namespace peloton
