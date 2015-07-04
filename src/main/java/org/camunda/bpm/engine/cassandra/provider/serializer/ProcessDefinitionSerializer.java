package org.camunda.bpm.engine.cassandra.provider.serializer;

import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

public class ProcessDefinitionSerializer implements CassandraSerializer<ProcessDefinitionEntity> {

  public void write(SettableData<?> data, ProcessDefinitionEntity entity) {
    data.setString("id", entity.getId());
    data.setString("key", entity.getKey());
    data.setInt("version", entity.getVersion());
    data.setString("category", entity.getCategory());
    data.setString("name", entity.getName());
    data.setString("deployment_id", entity.getDeploymentId());
    data.setInt("suspension_state", entity.getSuspensionState());  
  }

  public ProcessDefinitionEntity read(GettableData data) {
    ProcessDefinitionEntity entity = new ProcessDefinitionEntity();
    entity.setId(data.getString("id"));
    entity.setKey(data.getString("key"));
    entity.setVersion(data.getInt("version"));
    entity.setCategory(data.getString("category"));
    entity.setName(data.getString("name"));
    entity.setDeploymentId(data.getString("deployment_id"));
    entity.setSuspensionState(data.getInt("suspension_state"));
    return entity;
  }

}
