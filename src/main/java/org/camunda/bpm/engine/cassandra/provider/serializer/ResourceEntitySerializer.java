package org.camunda.bpm.engine.cassandra.provider.serializer;

import java.nio.ByteBuffer;

import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.utils.Bytes;

public class ResourceEntitySerializer implements CassandraSerializer<ResourceEntity> {
  
  public void write(SettableData<?> data, ResourceEntity entity) {
    data.setString("id", entity.getId());
    data.setString("name", entity.getName());
    data.setString("deployment_id", entity.getDeploymentId());
    data.setBytes("content", ByteBuffer.wrap(entity.getBytes()));
  }

  public ResourceEntity read(GettableData data) {
    ResourceEntity resourceEntity = new ResourceEntity();
    resourceEntity.setId(data.getString("id"));
    resourceEntity.setDeploymentId(data.getString("deployment_id"));
    resourceEntity.setName(data.getString("name"));
    resourceEntity.setBytes(Bytes.getArray(data.getBytes("content")));
    return resourceEntity;
  }

  @Override
  public ResourceEntity copy(ResourceEntity data) {
    //TODO
    throw new UnsupportedOperationException();
  }

}
