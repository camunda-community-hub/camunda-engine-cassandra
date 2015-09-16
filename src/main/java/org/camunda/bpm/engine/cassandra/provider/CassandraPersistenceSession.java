package org.camunda.bpm.engine.cassandra.provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.camunda.bpm.engine.OptimisticLockingException;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.indexes.AbstractIndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.AbstractOrderedIndexHandler;
import org.camunda.bpm.engine.cassandra.provider.operation.BulkDeleteDeployment;
import org.camunda.bpm.engine.cassandra.provider.operation.BulkDeleteJobDefinitionsByProcessDefinitionId;
import org.camunda.bpm.engine.cassandra.provider.operation.BulkDeleteProcessDefinitionByDeploymentId;
import org.camunda.bpm.engine.cassandra.provider.operation.BulkDeleteResourcesByDeploymentId;
import org.camunda.bpm.engine.cassandra.provider.operation.BulkOperationHandler;
import org.camunda.bpm.engine.cassandra.provider.operation.CompositeEntityLoader;
import org.camunda.bpm.engine.cassandra.provider.operation.DeploymentOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.EntityOperationHandler;
import org.camunda.bpm.engine.cassandra.provider.operation.EventSubscriptionOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.ExecutionEntityOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.JobDefinitionOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.JobOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.LoadedCompositeEntity;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessDefinitionOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader;
import org.camunda.bpm.engine.cassandra.provider.operation.ResourceOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.VariableEntityOperations;
import org.camunda.bpm.engine.cassandra.provider.query.SelectEventSubscriptionsByExecutionAndType;
import org.camunda.bpm.engine.cassandra.provider.query.SelectExclusiveJobsToExecute;
import org.camunda.bpm.engine.cassandra.provider.query.SelectExecutionsByQueryCriteria;
import org.camunda.bpm.engine.cassandra.provider.query.SelectJob;
import org.camunda.bpm.engine.cassandra.provider.query.SelectJobsByConfiguration;
import org.camunda.bpm.engine.cassandra.provider.query.SelectJobsByExecutionId;
import org.camunda.bpm.engine.cassandra.provider.query.SelectLatestProcessDefinitionByKeyQueryHandler;
import org.camunda.bpm.engine.cassandra.provider.query.SelectListQueryHandler;
import org.camunda.bpm.engine.cassandra.provider.query.SelectNextJobsToExecute;
import org.camunda.bpm.engine.cassandra.provider.query.SelectProcessDefinitionsByDeploymentId;
import org.camunda.bpm.engine.cassandra.provider.query.SelectProcessInstanceByQueryCriteria;
import org.camunda.bpm.engine.cassandra.provider.query.SingleResultQueryHandler;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.DeploymentEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.EventSubscriptionSerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.ExecutionEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.JobDefinitionEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.JobEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.ProcessDefinitionSerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.ResourceEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.serializer.VariableEntitySerializer;
import org.camunda.bpm.engine.cassandra.provider.table.DeploymentTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.IndexTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.JobDefinitionTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.JobTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.OrderedIndexTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.ResourceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.table.TableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.EventSubscriptionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.ExecutionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.VariableTypeHandler;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.db.AbstractPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbBulkOperation;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbEntityOperation;
import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.MessageEntity;
import org.camunda.bpm.engine.impl.persistence.entity.MessageEventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;
import org.camunda.bpm.engine.impl.persistence.entity.TimerEntity;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraPersistenceSession extends AbstractPersistenceSession {

  private final static Logger LOG = Logger.getLogger(CassandraPersistenceSession.class.getName());

  protected Session cassandraSession;

  protected StringBuilder batchBuilder = new StringBuilder();

  protected static List<TableHandler> tableHandlers = new ArrayList<TableHandler>();
  protected static Map<Class<?>, UDTypeHandler> udtHandlers = new HashMap<Class<?>, UDTypeHandler>();
  protected static Map<Class<?>, CassandraSerializer<?>> serializers = new HashMap<Class<?>, CassandraSerializer<?>>();
  protected static Map<String, CompositeEntityLoader> compositeEntitiyLoader = new HashMap<String, CompositeEntityLoader>();
  protected static Map<String, SingleResultQueryHandler<?>> singleResultQueryHandlers = new HashMap<String, SingleResultQueryHandler<?>>();
  protected static Map<String, SelectListQueryHandler<?, ?>> listResultQueryHandlers = new HashMap<String, SelectListQueryHandler<?,?>>();
  protected static Map<String, BulkOperationHandler> bulkOperationHandlers = new HashMap<String, BulkOperationHandler>();
  
  protected Map<Class<?>, EntityOperationHandler<?>> operations = new HashMap<Class<?>, EntityOperationHandler<?>>();

  protected BatchStatement varietyBatch = new BatchStatement();
  protected Map<String, LockedBatch<?>> lockedBatches = new HashMap<String, LockedBatch<?>>();
  protected Map<String, Map<String, LoadedCompositeEntity>> loadedEntityCache = new HashMap<String, Map<String, LoadedCompositeEntity>>();

  static {
    serializers.put(EventSubscriptionEntity.class, new EventSubscriptionSerializer());
    serializers.put(ExecutionEntity.class, new ExecutionEntitySerializer());
    serializers.put(ProcessDefinitionEntity.class, new ProcessDefinitionSerializer());
    serializers.put(ResourceEntity.class, new ResourceEntitySerializer());
    serializers.put(DeploymentEntity.class, new DeploymentEntitySerializer());
    serializers.put(VariableInstanceEntity.class, new VariableEntitySerializer());
    serializers.put(JobEntity.class, new JobEntitySerializer());
    serializers.put(JobDefinitionEntity.class, new JobDefinitionEntitySerializer());

    udtHandlers.put(ExecutionEntity.class, new ExecutionTypeHandler());
    udtHandlers.put(VariableInstanceEntity.class, new VariableTypeHandler());
    udtHandlers.put(EventSubscriptionEntity.class, new EventSubscriptionTypeHandler());

    tableHandlers.add(new ProcessDefinitionTableHandler());
    tableHandlers.add(new ResourceTableHandler());
    tableHandlers.add(new DeploymentTableHandler());
    tableHandlers.add(new ProcessInstanceTableHandler());
    tableHandlers.add(new IndexTableHandler());
    tableHandlers.add(new JobTableHandler());
    tableHandlers.add(new JobDefinitionTableHandler());
    tableHandlers.add(new OrderedIndexTableHandler());
    
    compositeEntitiyLoader.put(ProcessInstanceLoader.NAME, new ProcessInstanceLoader());
    
    singleResultQueryHandlers.put("selectLatestProcessDefinitionByKey", new SelectLatestProcessDefinitionByKeyQueryHandler());   
    singleResultQueryHandlers.put("selectJob", new SelectJob());   
    
    listResultQueryHandlers.put("selectExecutionsByQueryCriteria", new SelectExecutionsByQueryCriteria());
    listResultQueryHandlers.put("selectProcessInstanceByQueryCriteria", new SelectProcessInstanceByQueryCriteria());
    listResultQueryHandlers.put("selectEventSubscriptionsByExecutionAndType", new SelectEventSubscriptionsByExecutionAndType());
    listResultQueryHandlers.put("selectProcessDefinitionByDeploymentId", new SelectProcessDefinitionsByDeploymentId());
    listResultQueryHandlers.put("selectNextJobsToExecute", new SelectNextJobsToExecute());
    listResultQueryHandlers.put("selectJobsByConfiguration", new SelectJobsByConfiguration());
    listResultQueryHandlers.put("selectExclusiveJobsToExecute", new SelectExclusiveJobsToExecute());
    listResultQueryHandlers.put("selectJobsByExecutionId", new SelectJobsByExecutionId());
            
    bulkOperationHandlers.put("deleteDeployment", new BulkDeleteDeployment());
    bulkOperationHandlers.put("deleteResourcesByDeploymentId", new BulkDeleteResourcesByDeploymentId());
    bulkOperationHandlers.put("deleteProcessDefinitionsByDeploymentId", new BulkDeleteProcessDefinitionByDeploymentId());
    bulkOperationHandlers.put("deleteJobDefinitionsByProcessDefinitionId", new BulkDeleteJobDefinitionsByProcessDefinitionId());
    
  }

  protected boolean processInstanceVersionIncremented = false;
  
  /**
   * This method is called after the session is initialized, but before the engine finished initializing.
   * so it can be used for any initialization that requires cassandra session, 
   * but it should not use any camunda functionality
   * 
   * @param config
   */
  public static void staticInit(CassandraProcessEngineConfiguration config) {
    //TODO- add everything that needs to prepare statements
    EventSubscriptionOperations.prepare(config);
    ProcessDefinitionOperations.prepare(config);
    ResourceOperations.prepare(config);
    DeploymentOperations.prepare(config);
    ExecutionEntityOperations.prepare(config);
    VariableEntityOperations.prepare(config);
    JobOperations.prepare(config);
    JobDefinitionOperations.prepare(config);
    SelectNextJobsToExecute.prepare(config);  
    AbstractIndexHandler.prepare(config);
    AbstractOrderedIndexHandler.prepare(config);
  }

  public CassandraPersistenceSession(com.datastax.driver.core.Session session) {
    this.cassandraSession = session;
    //it is useful to keep context in operation for the duration of a single transaction, so not static
    operations.put(MessageEventSubscriptionEntity.class, new EventSubscriptionOperations(this));
    operations.put(ProcessDefinitionEntity.class, new ProcessDefinitionOperations(this));
    operations.put(ResourceEntity.class, new ResourceOperations(this));
    operations.put(DeploymentEntity.class, new DeploymentOperations(this));
    operations.put(ExecutionEntity.class, new ExecutionEntityOperations(this));
    operations.put(VariableInstanceEntity.class, new VariableEntityOperations(this));
    operations.put(JobEntity.class, new JobOperations(this));
    operations.put(MessageEntity.class, new JobOperations(this));
    operations.put(TimerEntity.class, new JobOperations(this));
    operations.put(JobDefinitionEntity.class, new JobDefinitionOperations(this));

  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public List<?> selectList(String statement, Object parameter) {
    LOG.log(Level.FINE, "selectList for statement '"+statement+"' parameter: "+parameter.toString());
    SelectListQueryHandler handler = listResultQueryHandlers.get(statement);
    if(handler == null) {
      LOG.log(Level.WARNING, "unhandled select statement '"+statement+"'");
      return Collections.emptyList();
    }
    else {
      return handler.executeQuery(this, parameter);
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends DbEntity> T selectById(Class<T> type, String id) {
    LOG.log(Level.FINE, "selectById for type '"+type.getSimpleName()+"' id '" +id+"'");

    EntityOperationHandler<?> entityOperations = operations.get(type);
    if(entityOperations != null) {
      DbEntity loadedEntity = entityOperations.getEntityById(this, id);
      fireEntityLoaded(loadedEntity);
      return (T) loadedEntity;
    }

    LOG.warning("Unhandled select by id "+type +" "+id);
    return null;
  }

  public LoadedCompositeEntity selectCompositeById(String compositeName, String id) {
    CompositeEntityLoader loader = compositeEntitiyLoader.get(compositeName);
    if(loader == null) {
      throw new ProcessEngineException("There is no composite loader for the composite named "+ compositeName);
    }

    if(loadedEntityCache.get(compositeName)!=null &&
        loadedEntityCache.get(compositeName).get(id)!=null){
      return loadedEntityCache.get(compositeName).get(id);
    }

    LoadedCompositeEntity composite = loader.getEntityById(this, id);
    if(composite == null) {
      return null;
    }

    if(loadedEntityCache.get(compositeName)==null){
      loadedEntityCache.put(compositeName, new HashMap<String, LoadedCompositeEntity>());
    }
    loadedEntityCache.get(compositeName).put(id, composite);

    processLoadedComposite(composite);
    return composite;
  }

  protected void processLoadedComposite(LoadedCompositeEntity composite) {
    DbEntity mainEntity = composite.getPrimaryEntity();
    boolean isMainEntityEventFired = false;
    for (Map<String, ? extends DbEntity> entities : composite.getEmbeddedEntities().values()) {
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
  }

  public Object selectOne(String statement, Object parameter) {
    LOG.log(Level.FINE, "selectOne for statement '"+statement+"' parameter: "+parameter.toString());

    SingleResultQueryHandler<?> queryHandler = singleResultQueryHandlers.get(statement);
    if(queryHandler != null) {
      DbEntity result = queryHandler.executeQuery(this, parameter);
      fireEntityLoaded(result);
      return result;
    }
    else if ("selectTableCount".equals(statement)) {
      @SuppressWarnings("unchecked")
      String tableName = ((Map<String, String>) parameter).get("tableName");
      return cassandraSession.execute(QueryBuilder.select().countAll().from(tableName)).one().getLong(0);
    }
    else {
      LOG.warning("unknown query "+statement);
      return null;
    }

  }

  public void lock(String statement, Object parameter) {
      LOG.warning("Lock called on statement: "+statement);
  }

  public void commit() {
    LOG.log(Level.FINE, "commit");
    //apply all batches in the transaction with the same timestamp
    long timestamp = ((CassandraProcessEngineConfiguration)Context.getProcessEngineConfiguration())
        .getCluster().getConfiguration().getPolicies().getTimestampGenerator().next();
    for (LockedBatch<?> batchWithLocking : lockedBatches.values()) {
      if(!batchWithLocking.isEmpty()){
        flushBatch(batchWithLocking.getBatch(), timestamp);
        flushBatch(batchWithLocking.getIndexBatch(), timestamp);
      }
    }
    if(!varietyBatch.getStatements().isEmpty()){
      flushBatch(varietyBatch, timestamp);
    }
  }

  private void flushBatch(BatchStatement batch, long timestamp) {
    if(batch==null){
      return;
    }
    batch.setDefaultTimestamp(timestamp);
    List<Row> rows = cassandraSession.execute(batch).all();
    for (Row row : rows) {
      if(!row.getBool("[applied]")) {
        LockedBatch lb = lockedBatches.values().iterator().next();
        
        LOG.log(Level.FINE, "flushBatch optimistic locking exception, version: "+ lb.getVersion());
        throw new OptimisticLockingException("Process instance was updated by another transaction concurrently.");
      }
    }
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
    LOG.log(Level.FINE, "insertEntity, operation: "+operation.toString());
    EntityOperationHandler entityOperations = operations.get(operation.getEntityType());
    if(entityOperations == null) {
      LOG.log(Level.WARNING, "unhandled INSERT '"+operation+"'");
    }
    else {
      entityOperations.insert(this, operation.getEntity());
    }

  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void deleteEntity(DbEntityOperation operation) {
    LOG.log(Level.FINE, "deleteEntity, operation: "+operation.toString());
    EntityOperationHandler entityOperations = operations.get(operation.getEntityType());
    if(entityOperations == null) {
      LOG.log(Level.WARNING, "unhandled DELETE '"+operation+"'");
    }
    else {
      entityOperations.delete(this, operation.getEntity());
    }
  }

  protected void deleteBulk(DbBulkOperation operation) {
    LOG.log(Level.FINE, "deleteBulk, operation: "+operation.toString());
    BulkOperationHandler handler = bulkOperationHandlers.get(operation.getStatement());
    if(handler == null) {
      LOG.log(Level.WARNING, "unhandled BULK delete '"+operation+"'");
    }
    else {
      handler.perform(this, operation.getParameter(), varietyBatch);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void updateEntity(DbEntityOperation operation) {
    LOG.log(Level.FINE, "updateEntity, operation: "+operation.toString());
    EntityOperationHandler entityOperations = operations.get(operation.getEntityType());
    if(entityOperations == null) {
      LOG.log(Level.WARNING, "unhandled UPDATE '"+operation+"'");
    }
    else {
      entityOperations.update(this, operation.getEntity());
    }
  }

  protected void updateBulk(DbBulkOperation operation) {
    LOG.log(Level.WARNING, "unhandled BULK update '"+operation+"'");
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

  @SuppressWarnings("unchecked")
  public static <T extends DbEntity> CassandraSerializer<T> getSerializer(Class<T> type) {
    return (CassandraSerializer<T>) serializers.get(type);
  }

  @SuppressWarnings("unchecked")
  public <T extends DbEntity> EntityOperationHandler<T> getOperationsHandler(Class<T> type) {
    return (EntityOperationHandler<T>) operations.get(type);
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

  public void addLockedBatch(String id, LockedBatch<?> batch) {
    lockedBatches.put(id, batch);
  }

  public void addStatement(Statement statement, String objectId) {
    if(statement==null){
      return;
    }
    LockedBatch<?> batch = lockedBatches.get(objectId);
    batch.addStatement(statement);
  }

  public void addIndexStatement(Statement statement, String objectId) {
    if(statement==null){
      return;
    }
    LockedBatch<?> batch = lockedBatches.get(objectId);
    batch.addIndexStatement(statement);
  }

  public void batchShouldNotLock(String objectId) {
    LockedBatch<?> batch = lockedBatches.get(objectId);
    batch.setShouldNotLock();
  }

  public void addStatement(Statement statement) {
    if(statement==null){
      return;
    }
    varietyBatch.add(statement);
  }

  /* (non-Javadoc)
   * @see org.camunda.bpm.engine.impl.db.AbstractPersistenceSession#dbSchemaCreateDmn()
   */
  @Override
  protected void dbSchemaCreateDmn() {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see org.camunda.bpm.engine.impl.db.AbstractPersistenceSession#dbSchemaDropDmn()
   */
  @Override
  protected void dbSchemaDropDmn() {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see org.camunda.bpm.engine.impl.db.AbstractPersistenceSession#isDmnTablePresent()
   */
  @Override
  public boolean isDmnTablePresent() {
    // TODO Auto-generated method stub
    return false;
  }
}
