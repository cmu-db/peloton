/*-------------------------------------------------------------------------
 *
 * peloton.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/peloton.c
 *
 *-------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>
#include <thread>
#include <map>

#include "backend/common/logger.h"
#include "backend/bridge/ddl/configuration.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_utils.h"
#include "backend/bridge/ddl/tests/bridge_test.h"
#include "backend/bridge/dml/executor/plan_executor.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/logging/log_manager.h"
#include "backend/gc/gc_manager_factory.h"

#include "postgres.h"
#include "c.h"
#include "access/xact.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "catalog/pg_namespace.h"
#include "executor/tuptable.h"
#include "libpq/ip.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"

#include "nodes/params.h"
#include "utils/guc.h"
#include "utils/errcodes.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/peloton.h"

#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"

/* ----------
 * Logging Flag
 * ----------
 */
bool logging_module_check = false;

bool syncronization_commit = false;

static void peloton_process_status(const peloton_status& status, const PlanState *planstate);

static void peloton_send_output(const peloton_status&  status,
                                bool sendTuples,
                                DestReceiver *dest);

static void __attribute__((unused)) peloton_test_config();

/* ----------
 * peloton_bootstrap -
 *
 *  Handle bootstrap requests in Peloton.
 * ----------
 */
void
peloton_bootstrap() {

  try {
    // Process the utility statement
    peloton::bridge::Bootstrap::BootstrapPeloton();
    // Sart logging
    if(logging_module_check == false){
      elog(DEBUG2, "....................................................................................................");
      elog(DEBUG2, "Logging Mode : %d", peloton_logging_mode);

      // Finished checking logging module
      logging_module_check = true;

      if(peloton_logging_mode != LOGGING_TYPE_INVALID) {

        // Launching a thread for logging
        auto& log_manager = peloton::logging::LogManager::GetInstance();
        if (!log_manager.IsInLoggingMode()) {

          // Set default logging mode
          log_manager.SetSyncCommit(true);
          elog(DEBUG2, "Wait for standby mode");

          // Wait for standby mode
          std::thread(&peloton::logging::LogManager::StartStandbyMode,
                      &log_manager).detach();
          log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_STANDBY, true);
          elog(DEBUG2, "Standby mode");

          // Do any recovery
          log_manager.StartRecoveryMode();
          elog(DEBUG2, "Wait for logging mode");

          // Wait for logging mode
          log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_LOGGING, true);
          elog(DEBUG2, "Logging mode");
        }

      }
    }
  }
  catch(const std::exception &exception) {
    elog(ERROR, "Peloton exception :: %s", exception.what());
  }

}

/* ----------
 * peloton_ddl -
 *
 *  Handle DDL requests in Peloton.
 * ----------
 */
void
peloton_ddl(Node *parsetree) {

  /* Ignore invalid parsetrees */
  if(parsetree == NULL || nodeTag(parsetree) == T_Invalid) {
    return;
  }

  try {
    /* Process the utility statement */
    peloton::bridge::DDL::ProcessUtility(parsetree);
  }
  catch(const std::exception &exception) {
    elog(ERROR, "Peloton exception :: %s", exception.what());
  }

}

/* ----------
 * peloton_dml -
 *
 *  Handle DML requests in Peloton.
 * ----------
 */
void
peloton_dml(const PlanState *planstate,
            bool sendTuples,
            DestReceiver *dest,
            TupleDesc tuple_desc,
            const char *prepStmtName) {
  peloton_status status;

  // Get the parameter list
  assert(planstate != NULL);
  assert(planstate->state != NULL);
  auto param_list = planstate->state->es_param_list_info;

  // Create the raw planstate info
  std::shared_ptr<const peloton::planner::AbstractPlan> mapped_plan_ptr;

  // Get our plan
  if (prepStmtName) {
    mapped_plan_ptr = peloton::bridge::PlanTransformer::GetInstance().GetCachedPlan(prepStmtName);
  }

  /* A cache miss or an unnamed plan */
  if (mapped_plan_ptr.get() == nullptr) {
    auto plan_state = peloton::bridge::DMLUtils::peloton_prepare_data(planstate);
    mapped_plan_ptr = peloton::bridge::PlanTransformer::GetInstance().TransformPlan(plan_state, prepStmtName);
  }

  // Ignore empty plans
  if(mapped_plan_ptr.get() == nullptr) {
    elog(WARNING, "Empty or unrecognized plan sent to Peloton");
    return;
  }

  std::vector<peloton::oid_t> target_list;
  std::vector<peloton::oid_t> qual;

  // Analyze the plan
  //if(rand() % 100 < 5)
  //  peloton::bridge::PlanTransformer::AnalyzePlan(plan, planstate);

  // Execute the plantree
  try {
    status = peloton::bridge::PlanExecutor::ExecutePlan(mapped_plan_ptr.get(),
                                                        param_list,
                                                        tuple_desc);

    // Clean up the plantree
    // Not clean up now ! This is cached !
    //peloton::bridge::PlanTransformer::CleanPlan(mapped_plan);
  }
  catch(const std::exception &exception) {
    elog(ERROR, "Peloton exception :: %s", exception.what());
  }

  // Wait for the response and process it
  peloton_process_status(status, planstate);

  // Send output to dest
  peloton_send_output(status, sendTuples, dest);

}

/* ----------
 * peloton_process_status() -
 *
 *  Process status.
 * ----------
 */
static void
peloton_process_status(const peloton_status& status, const PlanState *planstate) {
  int code;

  // Process the status code
  code = status.m_result;
  switch(code) {
    case peloton::RESULT_SUCCESS: {
      // TODO: Update stats ?
      planstate->state->es_processed = status.m_processed;
    }
    break;

    case peloton::RESULT_INVALID:
    case peloton::RESULT_FAILURE:
    default: {
      ereport(ERROR, (errcode(status.m_result),
          errmsg("transaction failed")));
    }
    break;
  }

}

/* ----------
 * peloton_send_output() -
 *
 *  Send the output to the receiver.
 * ----------
 */
void
peloton_send_output(const peloton_status& status,
                    bool sendTuples,
                    DestReceiver *dest) {
  TupleTableSlot *slot;

  // Go over any result slots
  if(status.m_result_slots != NULL)  {
    ListCell   *lc;

    foreach(lc, status.m_result_slots)
    {
      slot = (TupleTableSlot *) lfirst(lc);

      /*
       * if the tuple is null, then we assume there is nothing more to
       * process so we just end the loop...
       */
      if (TupIsNull(slot))
        break;

      /*
       * If we are supposed to send the tuple somewhere, do so. (In
       * practice, this is probably always the case at this point.)
       */
      if (sendTuples)
        (*dest->receiveSlot) (slot, dest);

      /*
       * Free the underlying heap_tuple
       * and the TupleTableSlot itself.
       */
      ExecDropSingleTupleTableSlot(slot);
    }

    // Clean up list
    list_free(status.m_result_slots);
  }
}

static void
peloton_test_config() {

  auto val = GetConfigOption("peloton_mode", false, false);
  elog(LOG, "Before SetConfigOption : %s", val);

  SetConfigOption("peloton_mode", "peloton_mode_1", PGC_USERSET, PGC_S_USER);

  val = GetConfigOption("peloton_mode", false, false);
  elog(LOG, "After SetConfigOption : %s", val);

  // Build the configuration map
  peloton::bridge::ConfigManager::BuildConfigMap();
}

/* ----------
 * IsPelotonQuery -
 *
 *  Does the query access peloton tables or not ?
 * ----------
 */
bool IsPelotonQuery(List *relationOids) {
  bool peloton_query = false;

  // Check if we are in Postmaster environment */
  if(IsPostmasterEnvironment == false && IsBackend == false) {
    return false;
  }

  if(relationOids != NULL) {
    ListCell   *lc;

    // Go over each relation on which the plan depends
    foreach(lc, relationOids) {
      Oid relation_id = lfirst_oid(lc);

      // Check if relation in public namespace
      Relation target_table = relation_open(relation_id, AccessShareLock);
      Oid target_table_namespace = target_table->rd_rel->relnamespace;

      if(target_table_namespace == PG_PUBLIC_NAMESPACE) {
        peloton_query = true;
      }

      relation_close(target_table, AccessShareLock);

      if(peloton_query == true)
        break;
    }
  }

  return peloton_query;
}

