package org.camunda.bpm.engine.cassandra.provider.serializer;

import java.util.Date;

import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

public class DeploymentEntitySerializer implements CassandraSerializer<DeploymentEntity> {

  public void write(SettableData<?> data, DeploymentEntity entity) {
    data.setString("id", entity.getId());
    data.setString("name", entity.getName());
    data.setLong("deploy_time", entity.getDeploymentTime().getTime());
  }

  public DeploymentEntity read(GettableData data) {
    DeploymentEntity deploymentEntity = new DeploymentEntity();
    deploymentEntity.setId(data.getString("id"));
    deploymentEntity.setName(data.getString("name"));
    deploymentEntity.setDeploymentTime(new Date(data.getLong("deploy_time")));
    return deploymentEntity;
  }

}
