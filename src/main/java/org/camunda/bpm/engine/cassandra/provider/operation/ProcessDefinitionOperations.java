package org.camunda.bpm.engine.cassandra.provider.operation;

import static org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler.TABLE_NAME;
import static org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler.TABLE_NAME_IDX_VERSION;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class ProcessDefinitionOperations extends AbstractEntityOperationHandler<ProcessDefinitionEntity> {
  
  private final static String INSERT = "INSERT into "+TABLE_NAME+" (id, key, version, category, name, deployment_id, suspension_state) "
      + "values "
      + "(?, ?, ?, ?, ?, ?, ?);";

  private final static String DELETE = "DELETE FROM "+TABLE_NAME+" WHERE id = '?';";

  //need a separate index table because it is a sorted index
  private final static String INSERT_IDX_VERSION = "INSERT into "+TABLE_NAME_IDX_VERSION+" (key, version, id) "
      + "values "
      + "(?, ?, ?);";
  
  private final static String DELETE_IDX_VERSION = "DELETE FROM "+TABLE_NAME_IDX_VERSION+" WHERE key = '?' AND version = ?;";

  private static PreparedStatement insertStatement=null;
  private static PreparedStatement insertVersionIndexStatement=null;
  private static PreparedStatement deleteStatement=null;
  private static PreparedStatement deleteVersionIndexStatement=null;
  private static boolean isInitialized = false;
  
  protected static Map<Class<?>, IndexHandler<ProcessDefinitionEntity>> indexHandlers = new HashMap<Class<?>, IndexHandler<ProcessDefinitionEntity>>();

  public void insert(CassandraPersistenceSession session, ProcessDefinitionEntity entity) {
    Session s = session.getSession();
    ensurePrepared(s);
    
    CassandraSerializer<ProcessDefinitionEntity> serializer = session.getSerializer(ProcessDefinitionEntity.class);
   
    // insert deployment
    BoundStatement statement = insertStatement.bind();    
    serializer.write(statement, entity);     
    session.addStatement(statement);
    
    // write index
    session.addStatement(insertVersionIndexStatement.bind(
        entity.getKey(),
        entity.getVersion(),
        entity.getId()));

    for(IndexHandler<ProcessDefinitionEntity> index:indexHandlers.values()){
      session.addStatement(index.getInsertStatement(session,entity));    
    }
  }

  public void delete(CassandraPersistenceSession session, ProcessDefinitionEntity entity) {
    ensurePrepared(session.getSession());
    session.addStatement(deleteStatement.bind(entity.getId()));
    session.addStatement(deleteVersionIndexStatement.bind(entity.getKey(), entity.getVersion()));    

    for(IndexHandler<ProcessDefinitionEntity> index:indexHandlers.values()){
      session.addStatement(index.getDeleteStatement(session,entity));    
    }
  }

  public void update(CassandraPersistenceSession session, ProcessDefinitionEntity entity) {
    
  }

  protected Class<ProcessDefinitionEntity> getEntityType() {
    return ProcessDefinitionEntity.class;
  }

  protected String getTableName() {
    return ProcessDefinitionTableHandler.TABLE_NAME;
  }

  private synchronized void ensurePrepared(Session session){
    if(!isInitialized){
      insertStatement = session.prepare(INSERT);
      insertVersionIndexStatement = session.prepare(INSERT_IDX_VERSION);
      deleteStatement = session.prepare(DELETE);
      deleteVersionIndexStatement = session.prepare(DELETE_IDX_VERSION);
      isInitialized=true;
    }
  }

  public static IndexHandler<ProcessDefinitionEntity> getIndexHandler(Class<?> type){
    return indexHandlers.get(type);
  }
}
