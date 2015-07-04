package org.camunda.bpm.engine.cassandra.provider.operation;

import static org.camunda.bpm.engine.cassandra.provider.table.ResourceTableHandler.TABLE_NAME;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;

public class ResourceOperations implements EntityOperationHandler<ResourceEntity> {

  protected final static String INSERT = "INSERT into "+TABLE_NAME+" (id, name, deployment_id, content) "
      + "values "
      + "(?, ?, ?, ?);";
  
  public void insert(CassandraPersistenceSession session, ResourceEntity entity) {

    Session s = session.getSession();

    CassandraSerializer<ResourceEntity> serializer = session.getSerializer(ResourceEntity.class);
   
    BoundStatement statement = s.prepare(INSERT).bind();
    serializer.write(statement, entity); 
    session.addStatement(statement);
  }

  public void delete(CassandraPersistenceSession session, ResourceEntity entity) {

  }

  public void update(CassandraPersistenceSession session, ResourceEntity entity) {

  }

}
