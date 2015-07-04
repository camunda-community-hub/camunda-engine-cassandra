package org.camunda.bpm.engine.cassandra.provider;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.impl.EventSubscriptionQueryValue;
import org.camunda.bpm.engine.impl.ExecutionQueryImpl;
import org.camunda.bpm.engine.impl.db.AbstractPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbBulkOperation;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbEntityOperation;
import org.camunda.bpm.engine.impl.history.parser.ProcessInstanceEndListener;
import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.MessageEventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

public class CassandraPersistenceSession extends AbstractPersistenceSession {
  
  private final static Logger LOG = Logger.getLogger(CassandraPersistenceSession.class.getName());

  protected Session cassandraSession;
  
  protected StringBuilder batchBuilder = new StringBuilder();

  protected static Map<Class<?>, TableHandler<?>> tableHandlers = new HashMap<Class<?>, TableHandler<?>>();
  protected static Map<Class<?>, TypeHandler<?>> typeHandlers = new HashMap<Class<?>, TypeHandler<?>>();
  
  protected BatchStatement batch = new BatchStatement();
  
  static {
    typeHandlers.put(ExecutionEntity.class, new ExecutionTypeHandler());
    typeHandlers.put(VariableInstanceEntity.class, new VariableTypeHandler());
    typeHandlers.put(EventSubscriptionEntity.class, new EventSubscriptionTypeHandler());
    
    tableHandlers.put(ProcessDefinitionEntity.class, new ProcessDefinitionTableHandler());
    tableHandlers.put(ResourceEntity.class, new ResourceTableHandler());
    tableHandlers.put(DeploymentEntity.class, new DeploymentTableHandler());
    tableHandlers.put(ExecutionEntity.class, new ProcessInstanceTableHandler());
    tableHandlers.put(MessageEventSubscriptionEntity.class, new EventSubscriptionTableHandler());
  }
  
  public CassandraPersistenceSession(com.datastax.driver.core.Session session) {
    this.cassandraSession = session;
  }

  public List<?> selectList(String statement, Object parameter) {

    if("selectExecutionsByQueryCriteria".equals(statement)) {
      ExecutionQueryImpl executionQuery = (ExecutionQueryImpl) parameter;
      if(executionQuery.getProcessInstanceId() == null) {
        throw new RuntimeException("Unsupported Execution Query: process instance id needs to be provided. Got: "+executionQuery);
      }
      ProcessInstanceTableHandler processInstanceTableHandler = getHandlerForType(ExecutionEntity.class);
      CassandraProcessInstance cpi = processInstanceTableHandler.findById(cassandraSession, executionQuery.getProcessInstanceId());
      
      List<EventSubscriptionQueryValue> eventSubscriptions = executionQuery.getEventSubscriptions();
      for (EventSubscriptionQueryValue eventSubscriptionQueryValue : eventSubscriptions) {
        for (EventSubscriptionEntity evtSubs : cpi.getEventSubscriptions().values()) {
         
        }
      };
    }
    
    
    LOG.log(Level.WARNING, "unhandled select statement '"+statement+"'");
    return Collections.emptyList();
    
  }

  @SuppressWarnings("unchecked")
  protected <T extends TableHandler<?>> T getHandlerForType(Class<?> type) {
    return (T) tableHandlers.get(type);
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
    
    TableHandler handler = tableHandlers.get(operation.getEntityType());
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
    return ProcessEngine.VERSION;
  }

  protected void dbSchemaCreateIdentity() {
    
  }

  protected void dbSchemaCreateHistory() {
    
  }

  
  protected void dbSchemaCreateEngine() {
    Collection<TypeHandler<?>> typeHandlers_ = typeHandlers.values();
    for (TypeHandler<?> typeHandler : typeHandlers_) {
      typeHandler.createType(cassandraSession);
    }
    
    Collection<TableHandler<?>> tableHandlers_ = tableHandlers.values();
    for (TableHandler<?> tableHandler : tableHandlers_) {
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
    
    Collection<TableHandler<?>> tableHandlers_ = tableHandlers.values();
    for (TableHandler<?> tableHandler : tableHandlers_) {
      tableHandler.dropTable(cassandraSession);
    }
    
    Collection<TypeHandler<?>> typeHandlers_ = typeHandlers.values();
    for (TypeHandler<?> typeHandler : typeHandlers_) {
      typeHandler.dropType(cassandraSession);
    }
    
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
