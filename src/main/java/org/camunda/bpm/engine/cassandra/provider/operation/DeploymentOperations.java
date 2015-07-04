package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.DeploymentTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class DeploymentOperations implements EntityOperations<DeploymentEntity> {

  protected final static String INSERT_STMNT = "INSERT into "+DeploymentTableHandler.TABLE_NAME+" (id, name, deploy_time) "
      + "values "
      + "(?, ?, ?);";
  
  public void insert(CassandraPersistenceSession session, DeploymentEntity entity, BatchStatement flush) {

    Session s = session.getSession();

    CassandraSerializer<DeploymentEntity> serializer = session.getSerializer(DeploymentEntity.class);
   
    BoundStatement statement = s.prepare(INSERT_STMNT).bind();
    
    serializer.write(statement, entity); 
    
    flush.add(statement);
  }

  public void delete(CassandraPersistenceSession session, DeploymentEntity entity, BatchStatement flush) {
    flush.add(QueryBuilder.delete().all().from(DeploymentTableHandler.TABLE_NAME).where(eq("id", entity.getId())));
  }

  public void update(CassandraPersistenceSession session, DeploymentEntity entity, BatchStatement flush) {
    
  }

}
