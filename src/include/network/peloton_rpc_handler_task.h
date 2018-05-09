//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_rpc_handler_task.h
//
// Identification: src/include/network/peloton_rpc_handler_task.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "capnp/ez-rpc.h"
#include "capnp/message.h"
#include "catalog/catalog.h"
#include "common/dedicated_thread_task.h"
#include "common/logger.h"
#include "common/internal_types.h"
#include "kj/debug.h"
#include "peloton/capnp/peloton_service.capnp.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace network {
class PelotonRpcServerImpl final : public PelotonService::Server {
 protected:
  kj::Promise<void> dropIndex(DropIndexContext request) override {
    auto database_oid = request.getParams().getRequest().getDatabaseOid();
    auto index_oid = request.getParams().getRequest().getIndexOid();
    LOG_DEBUG("Database oid: %d", database_oid);
    LOG_DEBUG("Index oid: %d", index_oid);

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Drop index. Fail if it doesn't exist.
    auto catalog = catalog::Catalog::GetInstance();
    try {
      catalog->DropIndex(database_oid, index_oid, txn);
    } catch (CatalogException e) {
      LOG_ERROR("Drop Index Failed");
      txn_manager.AbortTransaction(txn);
      return kj::NEVER_DONE;
    }
    txn_manager.CommitTransaction(txn);
    return kj::READY_NOW;
  }

  kj::Promise<void> createIndex(CreateIndexContext request) override {
    LOG_DEBUG("Received RPC to create index");
    auto database_oid = request.getParams().getRequest().getDatabaseOid();
    auto table_oid = request.getParams().getRequest().getTableOid();
    auto col_oids = request.getParams().getRequest().getKeyAttrOids();
    auto is_unique = request.getParams().getRequest().getUniqueKeys();
    LOG_DEBUG("Database oid: %d", database_oid);
    LOG_DEBUG("Table oid: %d", table_oid);

    std::stringstream sstream;
    sstream << database_oid << ":" << table_oid << ":";
    std::vector<oid_t> col_oid_vector;
    for (auto col : col_oids) {
      col_oid_vector.push_back(col);
      LOG_DEBUG("Col oid: %d", col);
      sstream << col << ",";
    }

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Create index. Fail if it already exists.
    auto catalog = catalog::Catalog::GetInstance();
    try {
      catalog->CreateIndex(database_oid, table_oid, col_oid_vector,
                           DEFUALT_SCHEMA_NAME, sstream.str(),
                           IndexType::BWTREE, IndexConstraintType::DEFAULT,
                           is_unique, txn);
    } catch (CatalogException e) {
      LOG_ERROR("Create Index Failed");
      txn_manager.AbortTransaction(txn);
      return kj::NEVER_DONE;
    }

    txn_manager.CommitTransaction(txn);
    return kj::READY_NOW;
  }
};

class PelotonRpcHandlerTask : public DedicatedThreadTask {
 public:
  explicit PelotonRpcHandlerTask(const char *address) : address_(address) {}

  void Terminate() override {
    // TODO(tianyu): Not implemented. See:
    // https://groups.google.com/forum/#!topic/capnproto/bgxCdqGD6oE
  }

  void RunTask() override {
    capnp::EzRpcServer server(kj::heap<PelotonRpcServerImpl>(), address_);
    LOG_DEBUG("Server listening on %s", address_);
    kj::NEVER_DONE.wait(server.getWaitScope());
  }

 private:
  const char *address_;
};
}  // namespace network
}  // namespace peloton
