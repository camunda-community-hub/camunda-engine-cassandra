package org.camunda.bpm.engine.cassandra.provider.serializer;

import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.MessageEntity;
import org.camunda.bpm.engine.impl.persistence.entity.TimerEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

public class JobEntitySerializer implements CassandraSerializer<JobEntity> {

  @Override
  public void write(SettableData<?> data, JobEntity entity) {
    data.setString("id", entity.getId());
    data.setString("type", entity.getType());
    data.setDate("due_date", entity.getDuedate());
    data.setDate("lock_exp_time", entity.getLockExpirationTime());
    data.setString("lock_owner", entity.getLockOwner());
    data.setBool("exclusive", entity.isExclusive());
    data.setString("execution_id", entity.getExecutionId());
    data.setString("process_instance_id", entity.getProcessInstanceId());
    data.setString("process_def_id", entity.getProcessDefinitionId());
    data.setString("process_def_key", entity.getProcessDefinitionKey());
    data.setInt("retries", entity.getRetries());
    data.setString("exception_stack_id", entity.getExceptionByteArrayId());
    data.setString("exception_message", entity.getExceptionMessage());
    if(entity instanceof TimerEntity){
      data.setString("repeat", ((TimerEntity) entity).getRepeat());
    }
    else{
      data.setString("repeat", null);
    }
    data.setString("handler_type", entity.getJobHandlerType());
    data.setString("handler_cfg", entity.getJobHandlerConfiguration());
    data.setString("deployment_id", entity.getDeploymentId());
    data.setInt("suspension_state", entity.getSuspensionState());
    data.setString("job_def_id", entity.getJobDefinitionId());
    data.setLong("sequence_counter", entity.getSequenceCounter());
    data.setInt("priority", entity.getPriority());
    data.setInt("revision", entity.getRevision());
  }

  @Override
  public JobEntity read(GettableData data) {
    JobEntity entity = null;
    String type = data.getString("type");
    if(MessageEntity.TYPE.equalsIgnoreCase(type)){
      entity = new MessageEntity();
    }
    else if(TimerEntity.TYPE.equalsIgnoreCase(type)){
      entity = new TimerEntity();
    }
    else {
      throw new IllegalArgumentException("Unknown job entity type "+type);
    }
    entity.setId(data.getString("id"));
    entity.setDuedate(data.getDate("due_date"));
    entity.setLockExpirationTime(data.getDate("lock_exp_time"));
    entity.setLockOwner(data.getString("lock_owner"));
    entity.setExclusive(data.getBool("exclusive"));
    entity.setExecutionId(data.getString("execution_id"));
    entity.setProcessInstanceId(data.getString("process_instance_id"));
    entity.setProcessDefinitionId(data.getString("process_def_id"));
    entity.setProcessDefinitionKey(data.getString("process_def_key"));
    entity.setRetries(data.getInt("retries"));
    //TODO - JobEntity does not allow to set the exception stack trace ID 
    //entity.setExceptionByteArrayId(data.getString("exception_stack_id"));
    entity.setExceptionMessage(data.getString("exception_message"));
    if(entity instanceof TimerEntity){
      ((TimerEntity) entity).setRepeat(data.getString("repeat"));
    }
    entity.setJobHandlerType(data.getString("handler_type"));
    entity.setJobHandlerConfiguration(data.getString("handler_cfg"));
    entity.setDeploymentId(data.getString("deployment_id"));
    entity.setSuspensionState(data.getInt("suspension_state"));
    entity.setJobDefinitionId(data.getString("job_def_id"));
    entity.setSequenceCounter(data.getLong("sequence_counter"));
    entity.setPriority(data.getInt("priority"));
    entity.setRevision(data.getInt("revision"));
    return entity;
  }

  @Override
  public JobEntity copy(JobEntity data) {
    JobEntity entity = null;
    String type = data.getType();
    if(MessageEntity.TYPE.equalsIgnoreCase(type)){
      entity = new MessageEntity();
    }
    else if(TimerEntity.TYPE.equalsIgnoreCase(type)){
      entity = new TimerEntity();
    }
    else {
      throw new IllegalArgumentException("Unknown job entity type "+type);
    }
    entity.setId(data.getId());
    entity.setDuedate(data.getDuedate());
    entity.setLockExpirationTime(data.getLockExpirationTime());
    entity.setLockOwner(data.getLockOwner());
    entity.setExclusive(data.isExclusive());
    entity.setExecutionId(data.getExecutionId());
    entity.setProcessInstanceId(data.getProcessInstanceId());
    entity.setProcessDefinitionId(data.getProcessDefinitionId());
    entity.setProcessDefinitionKey(data.getProcessDefinitionKey());
    entity.setRetries(data.getRetries());
    //TODO - JobEntity does not allow to set the exception stack trace ID 
    //entity.setExceptionByteArrayId(data.getExceptionByteArrayId());
    entity.setExceptionMessage(data.getExceptionMessage());
    if(entity instanceof TimerEntity){
      ((TimerEntity) entity).setRepeat(((TimerEntity) data).getRepeat());
    }
    entity.setJobHandlerType(data.getJobHandlerType());
    entity.setJobHandlerConfiguration(data.getJobHandlerConfiguration());
    entity.setDeploymentId(data.getDeploymentId());
    entity.setSuspensionState(data.getSuspensionState());
    entity.setJobDefinitionId(data.getJobDefinitionId());
    entity.setSequenceCounter(data.getSequenceCounter());
    entity.setPriority(data.getPriority());
    entity.setRevision(data.getRevision());
    return entity;
  }

}
