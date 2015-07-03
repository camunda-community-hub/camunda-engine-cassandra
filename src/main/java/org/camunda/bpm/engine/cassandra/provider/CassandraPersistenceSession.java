package org.camunda.bpm.engine.cassandra.provider;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.camunda.bpm.engine.impl.db.AbstractPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbBulkOperation;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbEntityOperation;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbOperation;
import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraPersistenceSession extends AbstractPersistenceSession {
  
  private final static Logger LOG = Logger.getLogger(CassandraPersistenceSession.class.getName());

  protected Session cassandraSession;
  
  protected StringBuilder batchBuilder = new StringBuilder();

  protected static Map<Class<?>, TableHandler<?>> handlers = new HashMap<Class<?>, TableHandler<?>>();
  
  protected BatchStatement batch = new BatchStatement();
  
  static {
    handlers.put(ProcessDefinitionEntity.class, new ProcessDefinitionTableHandler());
    handlers.put(ResourceEntity.class, new ResourceTableHandler());
    handlers.put(DeploymentEntity.class, new DeploymentTableHandler());
  }
  
  public CassandraPersistenceSession(com.datastax.driver.core.Session session) {
    this.cassandraSession = session;
  }

  public List<?> selectList(String statement, Object parameter) {

    
    
    LOG.log(Level.WARNING, "unhandled select statement '"+statement+"'");
    return Collections.emptyList();
    
  }

  @SuppressWarnings("unchecked")
  protected <T extends TableHandler<?>> T getHandlerForType(Class<?> type) {
    return (T) handlers.get(type);
  }

  public <T extends DbEntity> T selectById(Class<T> type, String id) {
    
    if(type == ExecutionEntity.class) {
      // first check cache
      // first check process instance table
      // if not found, check execution index => load process instance
      // else return null
    }
    return null;
  }

  public Object selectOne(String statement, Object parameter) {
    
    if("selectLatestProcessDefinitionByKey".equals(statement)) {
      ProcessDefinitionTableHandler handler = getHandlerForType(ProcessDefinitionEntity.class);
      return handler.selectLatestProcessDefinitionByKey(cassandraSession, (String) parameter);
    }
    
    return null;
  }

  public void lock(String statement, Object parameter) {
    
  }

  public void commit() {
    cassandraSession.execute(batch);
  }

  public void rollback() {
    
  }

  public void dbSchemaCheckVersion() {
    
  }

  public void flush() {
    
  }

  public void close() {
    
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void insertEntity(DbEntityOperation operation) {
    
    TableHandler handler = handlers.get(operation.getEntityType());
    if(handler == null) {
      LOG.log(Level.WARNING, "unhandled INSERT '"+operation+"'");
    }
    else {
      batch.addAll(handler.createInsertStatement(cassandraSession, (Object) operation.getEntity()));
    }     
    
  }

  protected void deleteEntity(DbEntityOperation operation) {
    LOG.log(Level.WARNING, "unhandled DELETE '"+operation+"'");
  }

  protected void deleteBulk(DbBulkOperation operation) {
  }

  protected void updateEntity(DbEntityOperation operation) {
    LOG.log(Level.WARNING, "unhandled UPDATE '"+operation+"'");
  }

  protected void updateBulk(DbBulkOperation operation) {
    
  }
  
  
  /// Schema mngt ///////////////////////////////////7

  protected String getDbVersion() {
    return null;
  }

  protected void dbSchemaCreateIdentity() {
    
  }

  protected void dbSchemaCreateHistory() {
    
  }

  
  protected void dbSchemaCreateEngine() {
    Collection<TableHandler<?>> tableHandlers = handlers.values();
    for (TableHandler<?> tableHandler : tableHandlers) {
      tableHandler.createTable(cassandraSession);
    }
  }

  protected void dbSchemaCreateCmmn() {
    
  }

  protected void dbSchemaCreateCmmnHistory() {
    
  }

  protected void dbSchemaDropIdentity() {
    
  }

  protected void dbSchemaDropHistory() {
    
  }

  protected void dbSchemaDropEngine() {
    
  }

  protected void dbSchemaDropCmmn() {
    
  }

  protected void dbSchemaDropCmmnHistory() {
    
  }

  public boolean isEngineTablePresent() {
    KeyspaceMetadata keyspaceMetaData = cassandraSession.getCluster()
      .getMetadata()
      .getKeyspace(cassandraSession.getLoggedKeyspace());
    
    return keyspaceMetaData.getTable(ProcessDefinitionTableHandler.TABLE_NAME) != null;
  }

  public boolean isHistoryTablePresent() {
    return false;
  }

  public boolean isIdentityTablePresent() {
    return false;
  }

  public boolean isCmmnTablePresent() {
    return false;
  }

  public boolean isCmmnHistoryTablePresent() {
    return false;
  }


}
