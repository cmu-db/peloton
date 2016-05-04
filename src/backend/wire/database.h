//
// Created by Zrs_y on 4/18/16.
//

#ifndef PELOTON_DATABASE_H
#define PELOTON_DATABASE_H

#include <vector>
#include <string>
#include <tuple>
#include "logger.h"
#include "wire.h"

namespace peloton {
namespace wiredb {
typedef std::pair<std::vector<unsigned char>, std::vector<unsigned char>> ResType;
// fieldinfotype: field name, oid (data type), size
typedef std::tuple<std::string, int, int> FieldInfoType;

#define WIRE_INTEGER 1
#define WIRE_TEXT 2
#define WIRE_FLOAT 3
#define WIRE_NULL 4

class DataBase {
public:
  DataBase() { }

  virtual ~DataBase() { }

  // virtual int PortalExec(const char *query, std::vector<ResType> &res, std::vector<FieldInfoType> &info, int &rows_change, std::string &errMsg) = 0;
};

}
}

#endif //PELOTON_DATABASE_H
