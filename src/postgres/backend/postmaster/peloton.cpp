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

#include "backend/networking/rpc_client.h"
#include "backend/common/logger.h"
#include "backend/common/serializer.h"
#include "backend/bridge/ddl/configuration.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_utils.h"
#include "backend/bridge/ddl/tests/bridge_test.h"
#include "backend/bridge/dml/executor/plan_executor.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/checkpoint_manager.h"
#include "backend/planner/seq_scan_plan.h"
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

// for memcached support
#include "access/printtup.h"

/* ----------
 * Logging Flag
 * ----------
 */
bool logging_module_check = false;

bool syncronization_commit = false;

static void peloton_process_status(const peloton_status& status, const PlanState *planstate);

static void peloton_send_output(const peloton_status&  status,
                                bool sendTuples,
                                DestReceiver *dest,
                                BackendContext* backend_state = nullptr);

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

    if(logging_module_check == false){
      elog(DEBUG2, "....................................................................................................");
      elog(DEBUG2, "Logging Mode : %d", peloton_logging_mode);

      // Finished checking logging module
      logging_module_check = true;

      auto& checkpoint_manager = peloton::logging::CheckpointManager::GetInstance();
      auto& log_manager = peloton::logging::LogManager::GetInstance();

      if (peloton_checkpoint_mode != CHECKPOINT_TYPE_INVALID) {
    	  // launch checkpoint thread
    	  if (!checkpoint_manager.IsInCheckpointingMode()) {

            // Wait for standby mode
            std::thread(&peloton::logging::CheckpointManager::StartStandbyMode,
                        &checkpoint_manager).detach();
            checkpoint_manager.WaitForModeTransition(peloton::CHECKPOINT_STATUS_STANDBY, true);
            elog(DEBUG2, "Standby mode");

            // Clean up table tile state before recovery from checkpoint
            log_manager.PrepareRecovery();

            // Do any recovery
            checkpoint_manager.StartRecoveryMode();
            elog(DEBUG2, "Wait for logging mode");

            // Wait for standby mode
            checkpoint_manager.WaitForModeTransition(peloton::CHECKPOINT_STATUS_DONE_RECOVERY, true);
            elog(DEBUG2, "Done recovery mode");
          }
      }

      if(peloton_logging_mode != LOGGING_TYPE_INVALID) {

        // Launching a thread for logging
        if (!log_manager.IsInLoggingMode()) {

          // Set default logging mode
          log_manager.SetSyncCommit(true);
          elog(DEBUG2, "Wait for standby mode");

          // Wait for standby mode
          std::thread(&peloton::logging::LogManager::StartStandbyMode,
                      &log_manager).detach();
          log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_STANDBY, true);
          elog(DEBUG2, "Standby mode");

          // Clean up database tile state before recovery from checkpoint
          log_manager.PrepareRecovery();

          // Do any recovery
          log_manager.StartRecoveryMode();
          elog(DEBUG2, "Wait for logging mode");

          // Wait for logging mode
          log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_LOGGING, true);
          elog(DEBUG2, "Logging mode");

          // Done recovery
          log_manager.DoneRecovery();
        }
      }

      // start checkpointing mode after recovery
      if (peloton_checkpoint_mode != CHECKPOINT_TYPE_INVALID) {
    	if (!checkpoint_manager.IsInCheckpointingMode()) {
    	  // Now, enter CHECKPOINTING mode
		  checkpoint_manager.SetCheckpointStatus(peloton::CHECKPOINT_STATUS_CHECKPOINTING);
		  elog(DEBUG2, "Checkpointing mode");
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
            const char *prepStmtName,
            BackendContext *backend_state) {
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

  // Construct params
  std::vector<peloton::Value> param_values = peloton::bridge::PlanTransformer::BuildParams(param_list);

  //===----------------------------------------------------------------------===//
  //   End for sending query
  //===----------------------------------------------------------------------===//

  // Ignore empty plans
  if(mapped_plan_ptr.get() == nullptr) {
    elog(WARNING, "Empty or unrecognized plan sent to Peloton");
    return;
  }

  std::vector<peloton::oid_t> target_list;
  std::vector<peloton::oid_t> qual;

  // Execute the plantree mapped_plan_ptr.get()
  try {
    status = peloton::bridge::PlanExecutor::ExecutePlan(mapped_plan_ptr.get(),
                                                        param_values,
                                                        tuple_desc);
  }
  catch(const std::exception &exception) {
    elog(ERROR, "Peloton exception :: %s", exception.what());
  }

  // Wait for the response and process it
  peloton_process_status(status, planstate);

  // Send output to dest
  peloton_send_output(status, sendTuples, dest, backend_state);

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
                    DestReceiver *dest,
                    BackendContext* backend_state) {
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

      // for memcached, directly call printtup
      if (sendTuples && backend_state) {
        printtup(slot, dest, backend_state);
      }
      else if (sendTuples)
        // otherwise use dest fp
        (*dest->receiveSlot) (slot, dest, backend_state);

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

//===--------------------------------------------------------------------===//
// Serialization/Deserialization
//===--------------------------------------------------------------------===//

  /**
   * The peloton_status has the following members:
   * m_processed   : uint32
   * m_result      : enum Result
   * m_result_slots: list pointer (type, length, data)
   *
   * Therefore a peloton_status is serialized as:
   * [(int) total size]
   * [(int) m_processed]
   * [(int8_t) m_result]
   * [(int8_t) note type]
   * [(int) list length]
   * [(bytes) data]
   * [(bytes) data]
   * [(bytes) data]
   * .....
   *
   * TODO: parent_ seems never be set or used
   */

bool peloton_status::SerializeTo(peloton::SerializeOutput &output) {

    // A placeholder for the total size written at the end
    int start = output.Position();
    output.WriteInt(-1);

    // Write m_processed.
    output.WriteInt(static_cast<int>(m_processed));

    // Write m_result, which is enum Result
    output.WriteByte(static_cast<int8_t>(m_result));

    if (m_result_slots != NULL) {
        // Write the list type
        NodeTag list_type = m_result_slots->type;
        output.WriteByte(static_cast<int8_t>(list_type));

        // Write the list length
        int list_length = m_result_slots->length;
        output.WriteInt(list_length);

        // Write the list data one by one
        ListCell *lc;
        foreach(lc, m_result_slots) {
            lfirst(lc);
            // TODO: Write the tuple into the buffer
        }
    } else {
        // Write the list type
        output.WriteByte(static_cast<int8_t>(T_Invalid));

        // Write the list length
        output.WriteInt(-1);
    }

    // Write the total length
    int32_t sz = static_cast<int32_t>(output.Position() - start - sizeof(int));
    assert(sz > 0);
    output.WriteIntAt(start, sz);

    return true;
}

/**
 * TODO: Deserialize
 */
bool peloton_status::DeserializeFrom(peloton::SerializeInputBE &input) {

//    List *slots = NULL;
//    slots = lappend(slots, slot);

    return true;
}
