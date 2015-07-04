package org.camunda.bpm.engine.cassandra.provider.serializer;

import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

public class ExecutionEntitySerializer implements CassandraSerializer<ExecutionEntity> {

  public void write(SettableData<?> data, ExecutionEntity entity) {
    data.setString("id", entity.getId())
    .setString("proc_inst_id", entity.getProcessInstanceId())
    .setString("parent_id", entity.getParentId())
    .setString("proc_def_id", entity.getProcessDefinitionId())
    .setString("super_exec", entity.getSuperExecutionId())
    .setString("super_case_exec", entity.getSuperCaseExecutionId())
    .setString("case_inst_id", entity.getCaseInstanceId())
    .setString("act_inst_id", entity.getActivityInstanceId())
    .setString("act_id", entity.getActivityId())
    .setBool("is_active", entity.isActive())
    .setBool("is_concurrent", entity.isConcurrent())
    .setBool("is_scope", entity.isScope())
    .setBool("is_event_scope", entity.isEventScope())
    .setInt("suspension_state", entity.getSuspensionState())
    .setInt("cached_ent_state", entity.getCachedEntityState())
    .setLong("sequence_counter", entity.getSequenceCounter());
  }

  public ExecutionEntity read(GettableData data) {
    ExecutionEntity executionEntity = new ExecutionEntity();
    executionEntity.setId(data.getString("id"));
    executionEntity.setProcessInstanceId(data.getString("proc_inst_id"));
    executionEntity.setParentId(data.getString("parent_id"));
    executionEntity.setProcessDefinitionId(data.getString("proc_def_id"));
    executionEntity.setSuperExecutionId(data.getString("super_exec"));
    executionEntity.setSuperCaseExecutionId(data.getString("super_case_exec"));
    executionEntity.setCaseInstanceId(data.getString("case_inst_id"));
    executionEntity.setActivityInstanceId(data.getString("act_inst_id"));
    executionEntity.setActivityId(data.getString("act_id"));
    executionEntity.setActive(data.getBool("is_active"));
    executionEntity.setConcurrent(data.getBool("is_concurrent"));
    executionEntity.setScope(data.getBool("is_scope"));
    executionEntity.setEventScope(data.getBool("is_event_scope"));
    executionEntity.setSuspensionState(data.getInt("suspension_state"));
    executionEntity.setCachedEntityState(data.getInt("cached_ent_state"));
    executionEntity.setSequenceCounter(data.getLong("sequence_counter"));

    return executionEntity;
  }

}
