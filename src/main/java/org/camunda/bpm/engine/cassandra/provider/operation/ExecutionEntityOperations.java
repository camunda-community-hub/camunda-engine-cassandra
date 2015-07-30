package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExecutionIdByProcessIdIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.ProcessIdByBusinessKeyIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.ProcessIdByExecutionIdIndex;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class ExecutionEntityOperations implements EntityOperationHandler<ExecutionEntity>{
  protected final static String INSERT = "INSERT into "+ProcessInstanceTableHandler.TABLE_NAME+" (id, version, business_key) "
      + "values "
      + "(?, ?, ?);";
  
  protected static Map<Class<?>, IndexHandler<ExecutionEntity>> indexHandlers = new HashMap<Class<?>, IndexHandler<ExecutionEntity>>();

  static {
    indexHandlers.put(ProcessIdByBusinessKeyIndex.class, new ProcessIdByBusinessKeyIndex());
    indexHandlers.put(ProcessIdByExecutionIdIndex.class, new ProcessIdByExecutionIdIndex());
    indexHandlers.put(ExecutionIdByProcessIdIndex.class, new ExecutionIdByProcessIdIndex());
  }
  
  public void insert(CassandraPersistenceSession session, ExecutionEntity entity) {
    
    Session s = session.getSession();
    
    if(entity.isProcessInstanceExecution()) {
      session.addStatement(s.prepare(INSERT).bind(
          entity.getId(),
          entity.getRevision(),
          entity.getBusinessKey()));
    }
    
    session.addStatement(createUpdateStatement(session, entity));

    for(IndexHandler<ExecutionEntity> index:indexHandlers.values()){
      session.addStatement(index.getInsertStatement(session,entity));    
    }
  }

  public void delete(CassandraPersistenceSession session, ExecutionEntity entity) {
    
    if(entity.isProcessInstanceExecution()) {
      session.addStatement(QueryBuilder.delete().all()
          .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId()))
          .onlyIf(eq("version", entity.getRevision())),
          entity.getProcessInstanceId());
      session.batchShouldNotLock(entity.getProcessInstanceId());
    }
    else {
      session.addStatement(QueryBuilder.delete().mapElt("executions", entity.getId())
          .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId())),
          entity.getProcessInstanceId());
    }
    
    for(IndexHandler<ExecutionEntity> index:indexHandlers.values()){
      session.addIndexStatement(index.getDeleteStatement(session,entity), entity.getProcessInstanceId());    
    }
  }

  public void update(CassandraPersistenceSession session, ExecutionEntity entity) {
    session.addStatement(createUpdateStatement(session, entity), entity.getProcessInstanceId());

    for(IndexHandler<ExecutionEntity> index:indexHandlers.values()){
      session.addIndexStatement(index.getInsertStatement(session,entity), entity.getProcessInstanceId());    
    }
  }

  protected Statement createUpdateStatement(CassandraPersistenceSession session, ExecutionEntity entity) {
    Session s = session.getSession();

    UDTypeHandler typeHander = session.getTypeHander(ExecutionEntity.class);
    CassandraSerializer<ExecutionEntity> serializer = session.getSerializer(ExecutionEntity.class);
    
    UDTValue value = typeHander.createValue(s);
    serializer.write(value, entity);
    
    return QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(put("executions", entity.getId(), value))
        .where(eq("id", entity.getProcessInstanceId()));
  }

  @Override
  public ExecutionEntity getEntityById(CassandraPersistenceSession session, String id) {    
    String procId = indexHandlers.get(ProcessIdByExecutionIdIndex.class).getUniqueValue(session, id);
    if(procId==null){
      return null;
    }
    LoadedCompositeEntity loadedCompostite = session.selectCompositeById(ProcessInstanceLoader.NAME, procId);
    if(loadedCompostite==null){
      return null;
    }
    if(procId.equals(id)){
      return (ExecutionEntity) loadedCompostite.getPrimaryEntity();
    }
    return (ExecutionEntity) loadedCompostite.get(ProcessInstanceLoader.EXECUTIONS).get(id);
  }

  public static IndexHandler<ExecutionEntity> getIndexHandler(Class<?> type){
    return indexHandlers.get(type);
  }
}
