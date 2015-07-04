package org.camunda.bpm.engine.cassandra.provider;

import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

public class ExecutionTypeHandler implements TypeHandler<ExecutionEntity> {

  public final static String TYPE_NAME = "execution";
    
  public final static String CREATE = "CREATE TYPE IF NOT EXISTS "+TYPE_NAME+" ("
      + "id text, "
      + "proc_inst_id text, "
      + "parent_id text, "
      + "proc_def_id text, "
      + "super_exec text, "
      + "super_case_exec text, "
      + "case_inst_id text, "
      + "act_inst_id text, "
      + "act_id text, "
      + "is_active boolean, "
      + "is_concurrent boolean, "
      + "is_scope boolean, "
      + "is_event_scope boolean, "
      + "suspension_state int, "
      + "cached_ent_state int, "
      + "sequence_counter bigint, "
      + ");";
  
  public final static String DROP = "DROP TYPE IF EXISTS "+TYPE_NAME+";";
  
  public void createType(Session s) {
    s.execute(CREATE);
  }

  public void dropType(Session s) {
    s.execute(DROP);
  }
  
  public UDTValue createValue(Session s, ExecutionEntity e) {
    UserType userType = s.getCluster().getMetadata().getKeyspace(s.getLoggedKeyspace())
     .getUserType(TYPE_NAME);
    
    return userType.newValue()
      .setString("id", e.getId())
      .setString("proc_inst_id", e.getProcessInstanceId())
      .setString("parent_id", e.getParentId())
      .setString("proc_def_id", e.getProcessDefinitionId())
      .setString("super_exec", e.getSuperExecutionId())
      .setString("super_case_exec", e.getSuperCaseExecutionId())
      .setString("case_inst_id", e.getCaseInstanceId())
      .setString("act_inst_id", e.getActivityInstanceId())
      .setString("act_id", e.getActivityId())
      .setBool("is_active", e.isActive())
      .setBool("is_concurrent", e.isConcurrent())
      .setBool("is_scope", e.isScope())
      .setBool("is_event_scope", e.isEventScope())
      .setInt("suspension_state", e.getSuspensionState())
      .setInt("cached_ent_state", e.getCachedEntityState())
      .setLong("sequence_counter", e.getSequenceCounter())
      ;
  }

  public ExecutionEntity deserializeValue(UDTValue serializedExecution) {
    ExecutionEntity executionEntity = new ExecutionEntity();
    executionEntity.setId(serializedExecution.getString("id"));
    executionEntity.setProcessInstanceId(serializedExecution.getString("proc_inst_id"));
    executionEntity.setParentId(serializedExecution.getString("parent_id"));
    executionEntity.setProcessDefinitionId(serializedExecution.getString("proc_def_id"));
    executionEntity.setSuperExecutionId(serializedExecution.getString("super_exec"));
    executionEntity.setSuperCaseExecutionId(serializedExecution.getString("super_case_exec"));
    executionEntity.setCaseInstanceId(serializedExecution.getString("case_inst_id"));
    executionEntity.setActivityInstanceId(serializedExecution.getString("act_inst_id"));
    executionEntity.setActivityId(serializedExecution.getString("act_id"));
    executionEntity.setActive(serializedExecution.getBool("is_active"));
    executionEntity.setConcurrent(serializedExecution.getBool("is_concurrent"));
    executionEntity.setScope(serializedExecution.getBool("is_scope"));
    executionEntity.setEventScope(serializedExecution.getBool("is_event_scope"));
    executionEntity.setSuspensionState(serializedExecution.getInt("suspension_state"));
    executionEntity.setCachedEntityState(serializedExecution.getInt("cached_ent_state"));
    executionEntity.setSequenceCounter(serializedExecution.getLong("sequence_counter"));
    return executionEntity;
  }
  
}
