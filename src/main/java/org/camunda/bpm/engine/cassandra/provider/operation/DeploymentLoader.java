package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.DeploymentTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class DeploymentLoader implements SingleEntityLoader<DeploymentEntity> {

  public DeploymentEntity getEntityById(CassandraPersistenceSession cassandraPersistenceSession, String id) {
    
    Session s = cassandraPersistenceSession.getSession();
    Row row = s.execute(select().all().from(DeploymentTableHandler.TABLE_NAME).where(eq("id", id))).one();
    if(row == null) {
      return null;
    }
    
    CassandraSerializer<DeploymentEntity> serializer = cassandraPersistenceSession.getSerializer(DeploymentEntity.class);
    DeploymentEntity deploymentEntity = serializer.read(row);
    
    return deploymentEntity;
  
  }

}
