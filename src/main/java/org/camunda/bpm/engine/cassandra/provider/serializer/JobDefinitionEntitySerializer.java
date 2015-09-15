package org.camunda.bpm.engine.cassandra.provider.serializer;

import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

public class JobDefinitionEntitySerializer implements CassandraSerializer<JobDefinitionEntity> {

  @Override
  public void write(SettableData<?> data, JobDefinitionEntity entity) {
    data.setString("id", entity.getId());
    data.setString("proc_def_id", entity.getProcessDefinitionId());
    data.setString("proc_def_key", entity.getProcessDefinitionKey());
    data.setString("act_id", entity.getActivityId());
    data.setString("type", entity.getJobType());
    data.setString("config", entity.getJobConfiguration());
    if(entity.getOverridingJobPriority()!=null){
      data.setInt("priority", entity.getOverridingJobPriority());
    }
    else{
      data.setToNull("priority");
    }
    data.setInt("suspension_state", entity.getSuspensionState());
    data.setInt("revision", entity.getRevision());
  }

  @Override
  public JobDefinitionEntity read(GettableData data) {
    JobDefinitionEntity entity = new JobDefinitionEntity();
    entity.setId(data.getString("id"));
    entity.setProcessDefinitionId(data.getString("proc_def_id"));
    entity.setProcessDefinitionKey(data.getString("proc_def_key"));
    entity.setActivityId(data.getString("act_id"));
    entity.setJobType(data.getString("type"));
    entity.setJobConfiguration(data.getString("config"));
    if(!data.isNull("priority")){
      entity.setJobPriority(data.getInt("priority"));
    }
    entity.setSuspensionState(data.getInt("suspension_state"));
    entity.setRevision(data.getInt("revision"));
    return entity;
  }

  @Override
  public JobDefinitionEntity copy(JobDefinitionEntity data) {
    JobDefinitionEntity entity = new JobDefinitionEntity();
    entity.setId(data.getId());
    entity.setProcessDefinitionId(data.getProcessDefinitionId());
    entity.setProcessDefinitionKey(data.getProcessDefinitionKey());
    entity.setActivityId(data.getActivityId());
    entity.setJobType(data.getJobType());
    entity.setJobConfiguration(data.getJobConfiguration());
    entity.setJobPriority(data.getOverridingJobPriority());
    entity.setSuspensionState(data.getSuspensionState());
    entity.setRevision(data.getRevision());
    return entity;
  }

}
