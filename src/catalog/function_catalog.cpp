//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_metrics_catalog.cpp
//
// Identification: src/catalog/query_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/function_catalog.h"
#include "catalog/catalog_defaults.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/foreach.hpp>

namespace peloton {
  namespace catalog {

    FunctionCatalog *FunctionCatalog::GetInstance(
        storage::Database *pg_catalog, type::AbstractPool *pool,
        concurrency::Transaction *txn) {
      static std::unique_ptr<FunctionCatalog> function_catalog(
          new FunctionCatalog(pg_catalog, pool,txn));

      return function_catalog.get();
    }

    FunctionCatalog::FunctionCatalog(
        storage::Database *pg_catalog,
        UNUSED_ATTRIBUTE type::AbstractPool *pool,
        UNUSED_ATTRIBUTE concurrency::Transaction *txn)
      : AbstractCatalog(FUNCTION_CATALOG_OID, FUNCTION_CATALOG_NAME,
          InitializeSchema().release(), pg_catalog) {
     
      //index created so we can query on name   
     Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, FUNCTION_CATALOG_NAME,
      {"function_name"}, FUNCTION_CATALOG_NAME "_skey0",
      false, IndexType::BWTREE, txn);
      }

    FunctionCatalog::~FunctionCatalog() {}

    /*@brief   private function for initialize schema of pg_database
     * @return  unqiue pointer to schema
     */
    std::unique_ptr<catalog::Schema> FunctionCatalog::InitializeSchema() {
      // convenience variables
      catalog::Constraint not_null_constraint(ConstraintType::NOTNULL, "not_null");
      auto integer_type_size = type::Type::GetTypeSize(type::Type::INTEGER);
      // TODO: not sure if these varchar size is ok, copied from old code
      auto varchar_type_size = type::Type::GetTypeSize(type::Type::VARCHAR);
      auto float_type_size = type::Type::GetTypeSize(type::Type::DECIMAL);
      auto bool_type_size = type::Type::GetTypeSize(type::Type::BOOLEAN);

      const std::string primary_key_constraint_name = "primary_key";
      // Add columns
      // Primary keys
      auto function_oid_column = catalog::Column(
          type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
          "function_oid", true);
      function_oid_column.AddConstraint(catalog::Constraint(
            ConstraintType::PRIMARY, primary_key_constraint_name));
      function_oid_column.AddConstraint(not_null_constraint);

      auto name_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "function_name", false);
      name_column.AddConstraint(not_null_constraint);

      auto owner_oid_column = catalog::Column(
          type::Type::INTEGER, integer_type_size, "owner_oid", true);

      auto lang_oid_column = catalog::Column(
          type::Type::INTEGER, integer_type_size, "lang_oid", true);
      lang_oid_column.AddConstraint(not_null_constraint);

      auto cost_column = catalog::Column(
          type::Type::DECIMAL, float_type_size , "cost", true);

      auto rows_column = catalog::Column(
          type::Type::DECIMAL, float_type_size , "rows", true);
      // Parameters
      auto variadic_oid_column = catalog::Column(
          type::Type::INTEGER, integer_type_size, "variadic_oid", true);

      auto isagg_column = catalog::Column(
          type::Type::BOOLEAN, bool_type_size, "isagg_column", true);

      auto iswindow_column = catalog::Column(
          type::Type::BOOLEAN, bool_type_size, "iswindow_column", true);

      auto secdef_column = catalog::Column(
          type::Type::BOOLEAN, bool_type_size, "secdef_column", true);

      auto leakproof_column = catalog::Column(
          type::Type::BOOLEAN, bool_type_size, "leakproof_column", true);

      auto isstrict_column = catalog::Column(
          type::Type::BOOLEAN, bool_type_size, "isstrict_column", true);

      auto retset_column = catalog::Column(
          type::Type::BOOLEAN, bool_type_size, "retset_column", true);
    
      auto volatile_column = catalog::Column(
          type::Type::VARCHAR, varchar_type_size, "volatile_column", false);

      auto num_args_column = catalog::Column(
          type::Type::INTEGER, integer_type_size, "num_params", true);
      num_args_column.AddConstraint(not_null_constraint);

      //might need a not null constraint?
      auto num_default_args_column = catalog::Column(
          type::Type::INTEGER, integer_type_size, "num_default_params", true);

      auto rettype_oid_column = catalog::Column(
          type::Type::INTEGER, integer_type_size, "rettype_oid", true);
      lang_oid_column.AddConstraint(not_null_constraint);

      auto arg_types_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "arg_types", false);
      name_column.AddConstraint(not_null_constraint);


      auto all_arg_types_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "all_arg_types", false);

      auto arg_modes_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "arg_modes", false);

      auto arg_names_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "arg_names", false);


      auto arg_defaults_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "arg_defaults", false);

      auto src_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "src", false);
      name_column.AddConstraint(not_null_constraint);

      auto bin_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "bin", false);
      
      auto config_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "config", false);

      auto aclitem_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
          "aclitem", false);

      std::unique_ptr<catalog::Schema> function_catalog_schema(
          new catalog::Schema({function_oid_column, name_column, owner_oid_column,
            lang_oid_column, cost_column, rows_column, variadic_oid_column,isagg_column, iswindow_column, secdef_column, leakproof_column, isstrict_column,retset_column, volatile_column, num_args_column, num_default_args_column,rettype_oid_column, arg_types_column, all_arg_types_column, arg_modes_column, arg_names_column, arg_defaults_column, src_column, bin_column, config_column, aclitem_column}));

      return function_catalog_schema;
    }

 const UDFFunctionData FunctionCatalog::GetFunction(const std::string &name,
                                   concurrency::Transaction *txn) {
  // Write logic to populate the fields of UDFFunctionData

  std::vector<oid_t> column_ids({1,16,17,22});
  oid_t index_offset = 1;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name,nullptr).Copy());
 
  UDFFunctionData function_info;
  function_info.func_is_present_ = false;

  auto result_tiles = GetResultWithIndexScan(column_ids, index_offset, values, txn);
  
   if (result_tiles->size() != 0) {
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      function_info.func_name_ = (*result_tiles)[0]
                      ->GetValue(0, 0)
                      .GetAs<oid_t>();  // After projection left 1 column

      function_info.return_type_ = (*result_tiles)[0]
                      ->GetValue(0, 1)
                      .GetAs<type::Type::TypeId>();  // After projection left 1 colum

      auto arg_types = (*result_tiles)[0]
                      ->GetValue(0, 2)
                      .GetAs<std::string>();  // After projection left 1 column
      std::vector<std::string> arg_types_split;

      boost::split(arg_types_split,arg_types, boost::is_any_of(" "));

      for(auto e : arg_types_split)
        function_info.argument_types_.push_back(static_cast<type::Type::TypeId>(std::stoi(e)));
      
      function_info.func_string_ = (*result_tiles)[0]
                      ->GetValue(0, 3)
                      .GetAs<std::string>();  // After projection left 1 column

      function_info.func_is_present_ = true;
    }
  }
  
  return function_info;
}

 ResultType  FunctionCatalog::InsertFunction(const std::string &proname,
                          UNUSED_ATTRIBUTE oid_t pronamespace, UNUSED_ATTRIBUTE oid_t proowner, 
                          oid_t prolang, UNUSED_ATTRIBUTE float procost,
                         UNUSED_ATTRIBUTE float prorows,UNUSED_ATTRIBUTE oid_t provariadic,UNUSED_ATTRIBUTE bool proisagg,
                         UNUSED_ATTRIBUTE bool proiswindow,UNUSED_ATTRIBUTE bool prosecdef,UNUSED_ATTRIBUTE bool proleakproof,
                         UNUSED_ATTRIBUTE bool proisstrict,UNUSED_ATTRIBUTE bool proretset,UNUSED_ATTRIBUTE char provolatile,
                          int pronargs,UNUSED_ATTRIBUTE int pronargdefaults, oid_t prorettype,
                          std::vector<type::Type::TypeId> proargtypes, 
                         UNUSED_ATTRIBUTE std::vector<type::Type::TypeId> proallargtypes,
                         UNUSED_ATTRIBUTE std::vector<int> proargmodes, 
                          std::vector<std::string> proargnames, 
                         UNUSED_ATTRIBUTE std::vector<type::Type::TypeId> proargdefaults, 
                          std::vector<std::string> prosrc_bin, 
                         UNUSED_ATTRIBUTE std::vector<std::string> proconfig, UNUSED_ATTRIBUTE std::vector<int> aclitem,
                          type::AbstractPool *pool,
                          concurrency::Transaction *txn){
      
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(catalog_table_->GetSchema(), true));

      //generate a unique oid
      auto oid = GetNextOid(); 
      auto val0 = type::ValueFactory::GetIntegerValue(oid);
      auto val1 = type::ValueFactory::GetVarcharValue(proname, nullptr);
      auto val2 = type::ValueFactory::GetNullValueByType(type::Type::INTEGER);
      auto val3 = type::ValueFactory::GetNullValueByType(type::Type::INTEGER);
      auto val4 = type::ValueFactory::GetIntegerValue(prolang);
      auto val5 = type::ValueFactory::GetNullValueByType(type::Type::DECIMAL);
      auto val6 = type::ValueFactory::GetNullValueByType(type::Type::DECIMAL);
      auto val7 = type::ValueFactory::GetNullValueByType(type::Type::INTEGER);
      auto val8 = type::ValueFactory::GetNullValueByType(type::Type::BOOLEAN);
      auto val9 = type::ValueFactory::GetNullValueByType(type::Type::BOOLEAN);
      auto val10 = type::ValueFactory::GetNullValueByType(type::Type::BOOLEAN);
      auto val11 = type::ValueFactory::GetNullValueByType(type::Type::BOOLEAN);
      auto val12 = type::ValueFactory::GetNullValueByType(type::Type::BOOLEAN);
      auto val13 = type::ValueFactory::GetNullValueByType(type::Type::BOOLEAN);
      auto val14 = type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
      auto val15 = type::ValueFactory::GetIntegerValue(pronargs);
      auto val16 = type::ValueFactory::GetNullValueByType(type::Type::INTEGER);
      auto val17 = type::ValueFactory::GetIntegerValue(prorettype);

      std::stringstream os;
      for(oid_t param_type : proargtypes)
            os << std::to_string(param_type) << "";
      
      auto val18 = type::ValueFactory::GetVarcharValue(os.str(), nullptr);
    
    
      auto val19 = type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
      auto val20 = type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
      auto val21 = type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
     
      std::stringstream os2;
      for(auto param_name : proargnames)
            os2 << param_name  << "";
      auto val22 = type::ValueFactory::GetVarcharValue(os2.str(), nullptr);

      auto val23 = type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
      auto val24 = type::ValueFactory::GetVarcharValue(prosrc_bin[0], nullptr);

      auto val25 =  type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);

      if(prosrc_bin.size() == 1) //only src, no binary
      {
        val25 = type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
      }
      else { 
        val25 = type::ValueFactory::GetVarcharValue(prosrc_bin[1], nullptr);
      }
     
      auto val26 = type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
      auto val27 = type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
 

      tuple->SetValue(0, val0, pool);
      tuple->SetValue(1, val1, pool);
      tuple->SetValue(2, val2, pool);
      tuple->SetValue(3, val3, pool);
      tuple->SetValue(4, val4, pool);
      tuple->SetValue(5, val5, pool);
      tuple->SetValue(6, val6, pool);
      tuple->SetValue(7, val7, pool);
      tuple->SetValue(8, val8, pool);
      tuple->SetValue(9, val9, pool);
      tuple->SetValue(10, val10, pool);
      tuple->SetValue(11, val11, pool);
      tuple->SetValue(12, val12, pool);
      tuple->SetValue(13, val13, pool);
      tuple->SetValue(14, val14, pool);
      tuple->SetValue(15, val15, pool);
      tuple->SetValue(16, val16, pool);
      tuple->SetValue(17, val17, pool);
      tuple->SetValue(18, val18, pool);
      tuple->SetValue(19, val19, pool);
      tuple->SetValue(20, val20, pool);
      tuple->SetValue(21, val21, pool);
      tuple->SetValue(22, val22, pool);
      tuple->SetValue(23, val23, pool);
      tuple->SetValue(24, val24, pool);
      tuple->SetValue(25, val25, pool);
      tuple->SetValue(26, val26, pool);
      tuple->SetValue(27, val27, pool);
   

      if(InsertTuple(std::move(tuple), txn) == true)
        return ResultType::SUCCESS;
      else
        return ResultType::FAILURE;
}

oid_t FunctionCatalog::GetFunctionOid(const std::string &function_name,
    concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // table_oid
  oid_t index_offset = 1;              // Index of name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(function_name, nullptr).Copy());

 // auto result = GetResultWithIndexScan(column_ids, index_offset, values, txn);

  auto result_tiles =
    GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t function_oid = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_name & database_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      function_oid = (*result_tiles)[0]
        ->GetValue(0, 0)
        .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return function_oid;
}

    

 

  }  // End catalog namespace
}  // End peloton namespace
