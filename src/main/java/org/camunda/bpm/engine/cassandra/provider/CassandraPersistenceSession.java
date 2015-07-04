package org.camunda.bpm.engine.cassandra.provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.cassandra.provider.operation.CompositeEntityLoader;
import org.camunda.bpm.engine.cassandra.provider.operation.DeploymentOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.EntityOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.EventSubscriptionOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.ExecutionEntityOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.LoadedCompositeEntity;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessDefinitionLoader;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessDefinitionOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader;
import org.camunda.bpm.engine.cassandra.provider.operation.ResourceOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.SingleEntityLoader;
import org.camunda.bpm.engine.cassandra.provider.query.SelectLatestProcessDefinitionByKeyQueryHandler;
import org.camunda.bpm.engine.cassandra.provider.query.SingleResultQueryHandler;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.DeploymentEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.EventSubscriptionSerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.ExecutionEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.ProcessDefinitionSerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.ResourceEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.table.DeploymentTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.ResourceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.TableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.EventSubscriptionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.ExecutionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.VariableTypeHandler;
import org.camunda.bpm.engine.impl.EventSubscriptionQueryValue;
import org.camunda.bpm.engine.impl.ExecutionQueryImpl;
import org.camunda.bpm.engine.impl.SingleQueryVariableValueCondition;
import org.camunda.bpm.engine.impl.db.AbstractPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbBulkOperation;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbEntityOperation;
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
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraPersistenceSession extends AbstractPersistenceSession {
  
  private final static Logger LOG = Logger.getLogger(CassandraPersistenceSession.class.getName());

  protected Session cassandraSession;
  
  protected StringBuilder batchBuilder = new StringBuilder();

  protected static List<TableHandler> tableHandlers = new ArrayList<TableHandler>();
  protected static Map<Class<?>, UDTypeHandler> udtHandlers = new HashMap<Class<?>, UDTypeHandler>();  
  protected static Map<Class<?>, CassandraSerializer<?>> serializers = new HashMap<Class<?>, CassandraSerializer<?>>();
  protected static Map<Class<?>, EntityOperations<?>> operations = new HashMap<Class<?>, EntityOperations<?>>();
  protected static Map<Class<?>, SingleEntityLoader<?>> singleEntityLoaders = new HashMap<Class<?>, SingleEntityLoader<?>>();
  protected static Map<String, CompositeEntityLoader> compositeEntitiyLoader = new HashMap<String, CompositeEntityLoader>();
  protected static Map<String, SingleResultQueryHandler<?>> singleResultQueryHandlers = new HashMap<String, SingleResultQueryHandler<?>>();
  
  protected BatchStatement batch = new BatchStatement();
  
  static {
    serializers.put(EventSubscriptionEntity.class, new EventSubscriptionSerializer());
    serializers.put(ExecutionEntity.class, new ExecutionEntitySerializer());
    serializers.put(ProcessDefinitionEntity.class, new ProcessDefinitionSerializer());
    serializers.put(ResourceEntity.class, new ResourceEntitySerializer());
    serializers.put(DeploymentEntity.class, new DeploymentEntitySerializer());
    
    udtHandlers.put(ExecutionEntity.class, new ExecutionTypeHandler());
    udtHandlers.put(VariableInstanceEntity.class, new VariableTypeHandler());
    udtHandlers.put(EventSubscriptionEntity.class, new EventSubscriptionTypeHandler());
    
    tableHandlers.add(new ProcessDefinitionTableHandler());
    tableHandlers.add(new ResourceTableHandler());
    tableHandlers.add(new DeploymentTableHandler());
    tableHandlers.add(new ProcessInstanceTableHandler());
    
    operations.put(MessageEventSubscriptionEntity.class, new EventSubscriptionOperations());
    operations.put(ProcessDefinitionEntity.class, new ProcessDefinitionOperations());
    operations.put(ResourceEntity.class, new ResourceOperations());
    operations.put(DeploymentEntity.class, new DeploymentOperations());
    operations.put(ExecutionEntity.class, new ExecutionEntityOperations());

    singleEntityLoaders.put(ProcessDefinitionEntity.class, new ProcessDefinitionLoader());
    
    compositeEntitiyLoader.put("processInstance", new ProcessInstanceLoader());
    
    singleResultQueryHandlers.put("selectLatestProcessDefinitionByKey", new SelectLatestProcessDefinitionByKeyQueryHandler());
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
      
      CompositeEntityLoader processInstanceLoader = compositeEntitiyLoader.get("processInstance");
      LoadedCompositeEntity loadedProcessInstance = processInstanceLoader.getEntityById(this, executionQuery.getProcessInstanceId());
      
      if(loadedProcessInstance == null) {
        return null;
      }
      
      processLoadedComposite(loadedProcessInstance, processInstanceLoader);
      
      List<EventSubscriptionQueryValue> eventSubscriptions = executionQuery.getEventSubscriptions();
      EventSubscriptionQueryValue eventSubscriptionQueryValue = eventSubscriptions.get(0);
      
      for (DbEntity entity : loadedProcessInstance.get("eventSubscriptions").values()) {
        EventSubscriptionEntity eventSubscriptionEntity = (EventSubscriptionEntity) entity;
        if(eventSubscriptionQueryValue.getEventName().equals(eventSubscriptionEntity.getEventName())) {
          return Arrays.asList(loadedProcessInstance.get("executions").get(eventSubscriptionEntity.getExecutionId()));
        }
      }
    }
    
    
    LOG.log(Level.WARNING, "unhandled select statement '"+statement+"'");
    return Collections.emptyList();
    
  }

  @SuppressWarnings("unchecked")
  public <T extends DbEntity> T selectById(Class<T> type, String id) {
    
    if(type.equals(ExecutionEntity.class)) {
      // special case:
      CompositeEntityLoader processInstanceLoader = compositeEntitiyLoader.get("processInstance");
      LoadedCompositeEntity loadedProcessInstance = processInstanceLoader.getEntityById(this, id);
      if(loadedProcessInstance != null) {
        return null;
      }
      return processLoadedComposite(loadedProcessInstance, processInstanceLoader);
    }
    else {
      SingleEntityLoader<?> singleEntityLoader = singleEntityLoaders.get(type);
      if(singleEntityLoader != null) {
        DbEntity loadedEntity = singleEntityLoader.getEntityById(this, id);
        fireEntityLoaded(loadedEntity);
        return (T) loadedEntity;
      }
    }
    
    LOG.warning("Unhandled select by id "+type +" "+id);
    return null;
  }


  @SuppressWarnings("unchecked")
  protected <T extends DbEntity> T processLoadedComposite(LoadedCompositeEntity composite, CompositeEntityLoader processInstanceLoader) {
    DbEntity mainEntity = composite.getMainEntity();
    boolean isMainEntityEventFired = false;
    for (Map<String, DbEntity> entities : composite.getEmbeddedEntities().values()) {
      for (DbEntity entity : entities.values()) {
        fireEntityLoaded(entity);
        if(entity == mainEntity) {
          isMainEntityEventFired = true;
        }
      }      
    }
    if(!isMainEntityEventFired) {
      fireEntityLoaded(mainEntity);
    }
    return (T) mainEntity;
  }

  public Object selectOne(String statement, Object parameter) {
    
    SingleResultQueryHandler<?> queryHandler = singleResultQueryHandlers.get(statement);
    if(queryHandler != null) {
      DbEntity result = queryHandler.executeQuery(this, parameter);
      fireEntityLoaded(result);
      return result;
    }
    else if ("selectTableCount".equals(statement)) {
      String tableName = ((Map<String, String>) parameter).get("tableName");
      return cassandraSession.execute(QueryBuilder.select().countAll().from(tableName)).one().getLong(0);
    }
    else {
      LOG.warning("unknown query "+statement);
      return null;
    }

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
    EntityOperations entityOperations = operations.get(operation.getEntityType());
    if(entityOperations == null) {
      LOG.log(Level.WARNING, "unhandled INSERT '"+operation+"'");
    }
    else {
      entityOperations.insert(this, operation.getEntity(), batch);
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
    Collection<UDTypeHandler> typeHandlers_ = udtHandlers.values();
    for (UDTypeHandler typeHandler : typeHandlers_) {
      typeHandler.createType(cassandraSession);
    }
    
    for (TableHandler tableHandler : tableHandlers) {
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
    for (TableHandler tableHandler : tableHandlers) {
      tableHandler.dropTable(cassandraSession);
    }
    
    Collection<UDTypeHandler> typeHandlers_ = udtHandlers.values();
    for (UDTypeHandler typeHandler : typeHandlers_) {
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
  
  public UDTypeHandler getTypeHander(Class<?> entityType) {
    return udtHandlers.get(entityType);
  }
  
  public <T extends DbEntity> CassandraSerializer<T> getSerializer(Class<T> type) {
    return (CassandraSerializer<T>) serializers.get(type);
  }
  
  public Session getSession() {
    return cassandraSession;
  }

  public List<String> getTableNamesPresent() {
    List<String> tableNames = new ArrayList<String>();
    for (TableHandler tableHandler : tableHandlers) {
      tableNames.addAll(tableHandler.getTableNames());
    }
    return tableNames;
  }

}
