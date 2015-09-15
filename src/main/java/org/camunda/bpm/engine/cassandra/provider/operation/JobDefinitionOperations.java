package org.camunda.bpm.engine.cassandra.provider.operation;

import static org.camunda.bpm.engine.cassandra.provider.table.JobDefinitionTableHandler.TABLE_NAME;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class JobDefinitionOperations extends AbstractEntityOperationHandler<JobDefinitionEntity> {
  
  private final static String INSERT = "INSERT into "+TABLE_NAME+" ("
      + "id, "
      + "proc_def_id, "
      + "proc_def_key, "
      + "act_id, "
      + "type, "
      + "config, "
      + "priority, "
      + "suspension_state, "
      + "revision "
      + ") values "
      + "(?, ?, ?, ?, ?, ?, ?, ?, ?);";

  private final static String DELETE = "DELETE FROM "+TABLE_NAME+" WHERE id = '?';";

  private static PreparedStatement insertStatement=null;
  private static PreparedStatement deleteStatement=null;
  
  protected static Map<Class<?>, IndexHandler<JobDefinitionEntity>> indexHandlers = new HashMap<Class<?>, IndexHandler<JobDefinitionEntity>>();

  public JobDefinitionOperations(CassandraPersistenceSession cassandraPersistenceSession) {
  } 

  public static void prepare(CassandraProcessEngineConfiguration config) {
      insertStatement = config.getSession().prepare(INSERT);
      deleteStatement = config.getSession().prepare(DELETE);
  }
  
  public void insert(CassandraPersistenceSession session, JobDefinitionEntity entity) {
    Session s = session.getSession();
    
    CassandraSerializer<JobDefinitionEntity> serializer = CassandraPersistenceSession.getSerializer(JobDefinitionEntity.class);
   
    // insert deployment
    BoundStatement statement = insertStatement.bind();    
    serializer.write(statement, entity);     
    session.addStatement(statement);
    
    for(IndexHandler<JobDefinitionEntity> index:indexHandlers.values()){
      session.addStatement(index.getInsertStatement(session,entity));    
    }
  }

  public void delete(CassandraPersistenceSession session, JobDefinitionEntity entity) {
    session.addStatement(deleteStatement.bind(entity.getId()));

    for(IndexHandler<JobDefinitionEntity> index:indexHandlers.values()){
      session.addStatement(index.getDeleteStatement(session,entity));    
    }
  }

  public void update(CassandraPersistenceSession session, JobDefinitionEntity entity) {
    insert(session,entity);
  }

  protected Class<JobDefinitionEntity> getEntityType() {
    return JobDefinitionEntity.class;
  }

  protected String getTableName() {
    return TABLE_NAME;
  }

  public static IndexHandler<JobDefinitionEntity> getIndexHandler(Class<?> type){
    return indexHandlers.get(type);
  }

}
