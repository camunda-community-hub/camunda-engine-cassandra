package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.DeploymentTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class DeploymentOperations extends AbstractEntityOperationHandler<DeploymentEntity> {

  protected final static String INSERT_STMNT = "INSERT into "+DeploymentTableHandler.TABLE_NAME+" (id, name, deploy_time) "
      + "values "
      + "(?, ?, ?);";
  
  public DeploymentOperations(CassandraPersistenceSession cassandraPersistenceSession) {
  }

  public static void prepare(CassandraProcessEngineConfiguration config) {
		// TODO - prepare all statements
  }

  public void insert(CassandraPersistenceSession session, DeploymentEntity entity) {

    Session s = session.getSession();

    CassandraSerializer<DeploymentEntity> serializer = session.getSerializer(DeploymentEntity.class);
   
    BoundStatement statement = s.prepare(INSERT_STMNT).bind();
    
    serializer.write(statement, entity); 
    
    session.addStatement(statement);
  }

  public void delete(CassandraPersistenceSession session, DeploymentEntity entity) {
    session.addStatement(QueryBuilder.delete().all().from(DeploymentTableHandler.TABLE_NAME).where(eq("id", entity.getId())));
  }

  public void update(CassandraPersistenceSession session, DeploymentEntity entity) {
    
  }

  @Override
  protected Class<DeploymentEntity> getEntityType() {
    return DeploymentEntity.class;
  }

  @Override
  protected String getTableName() {
    return DeploymentTableHandler.TABLE_NAME;
  }

}
