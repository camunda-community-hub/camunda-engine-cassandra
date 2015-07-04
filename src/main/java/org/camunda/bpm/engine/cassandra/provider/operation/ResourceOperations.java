package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static org.camunda.bpm.engine.cassandra.provider.table.ResourceTableHandler.TABLE_NAME;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ResourceTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;

public class ResourceOperations implements EntityOperations<ResourceEntity> {

  protected final static String INSERT = "INSERT into "+TABLE_NAME+" (id, name, deployment_id, content) "
      + "values "
      + "(?, ?, ?, ?);";
  
  public void insert(CassandraPersistenceSession session, ResourceEntity entity, BatchStatement flush) {

    Session s = session.getSession();

    CassandraSerializer<ResourceEntity> serializer = session.getSerializer(ResourceEntity.class);
   
    BoundStatement statement = s.prepare(INSERT).bind();
    serializer.write(statement, entity); 
    flush.add(statement);
  }

  public void delete(CassandraPersistenceSession session, ResourceEntity entity, BatchStatement flush) {

  }

  public void update(CassandraPersistenceSession session, ResourceEntity entity, BatchStatement flush) {

  }

}
