package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.AbstractVariableValueIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExecutionIdByVariableValueIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.ProcessIdByProcessVariableValueIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.ProcessIdByVariableIdIndex;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class VariableEntityOperations implements EntityOperationHandler<VariableInstanceEntity>{
  protected static Map<Class<?>, IndexHandler<VariableInstanceEntity>> indexHandlers = new HashMap<Class<?>, IndexHandler<VariableInstanceEntity>>();

  private Map<String, VariableInstanceEntity> varValuesCache=new HashMap<String,VariableInstanceEntity>();
  
  static {
    indexHandlers.put(ProcessIdByVariableIdIndex.class, new ProcessIdByVariableIdIndex());
    indexHandlers.put(ExecutionIdByVariableValueIndex.class, new ExecutionIdByVariableValueIndex());
    indexHandlers.put(ProcessIdByProcessVariableValueIndex.class, new ProcessIdByProcessVariableValueIndex());
  }
  

  public void insert(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    session.addStatement(createUpdateStatement(session, entity));

    for(IndexHandler<VariableInstanceEntity> index:indexHandlers.values()){
      session.addStatement(index.getInsertStatement(entity));    
    }
  }

  public void delete(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    session.addStatement(QueryBuilder.delete().mapElt("variables", entity.getId())
      .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId())), entity.getProcessInstanceId());
    
    for(IndexHandler<VariableInstanceEntity> index:indexHandlers.values()){
      session.addIndexStatement(index.getDeleteStatement(varValuesCache.get(entity.getId())), entity.getProcessInstanceId());  
    }
    varValuesCache.remove(entity.getId());
  }

  public void update(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    session.addStatement(createUpdateStatement(session, entity), entity.getProcessInstanceId());

    for(IndexHandler<VariableInstanceEntity> index:indexHandlers.values()){
      session.addIndexStatement(index.getInsertStatement(entity), entity.getProcessInstanceId());  
      if(index instanceof AbstractVariableValueIndex){
        session.addIndexStatement(index.getDeleteStatement(varValuesCache.get(entity.getId())), entity.getProcessInstanceId());
      }
    }
    updateVariableCache(entity);
  }

  protected Statement createUpdateStatement(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    Session s = session.getSession();

    UDTypeHandler typeHandler = session.getTypeHander(VariableInstanceEntity.class);
    CassandraSerializer<VariableInstanceEntity> serializer = session.getSerializer(VariableInstanceEntity.class);

    UDTValue value = typeHandler.createValue(s);
    serializer.write(value, entity);

    return QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
      .with(put("variables", entity.getId(), value))
      .where(eq("id", entity.getProcessInstanceId()));
  }

  @Override
  public VariableInstanceEntity getEntityById(CassandraPersistenceSession session, String id) {    
    String procId = indexHandlers.get(ProcessIdByVariableIdIndex.class).getUniqueValue(session, id);
    if(procId==null){
      return null;
    }
    LoadedCompositeEntity loadedComposite = session.selectCompositeById(ProcessInstanceLoader.NAME, procId);
    if(loadedComposite==null){
      return null;
    }
    return (VariableInstanceEntity) loadedComposite.get(ProcessInstanceLoader.VARIABLES).get(id);
  }

  public static IndexHandler<VariableInstanceEntity> getIndexHandler(Class<?> type){
    return indexHandlers.get(type);
  }
  
  public void onCompositeLoad(LoadedCompositeEntity composite){
    //store variable values in a local cache to ensure they can be deleted    
    //TODO - replace this quick hack with a better solution once the indexing approach is more finalised
    @SuppressWarnings("unchecked")
    Map<String, VariableInstanceEntity> variables=(Map<String, VariableInstanceEntity>) composite.get(ProcessInstanceLoader.VARIABLES);
    for(String id:variables.keySet()){
      updateVariableCache(variables.get(id));
    }
  }
  
  private void updateVariableCache(VariableInstanceEntity variable){
    VariableInstanceEntity copy= new VariableInstanceEntity();
    //just copy relevant fields
    copy.setId(variable.getId());
    copy.setExecutionId(variable.getExecutionId());
    copy.setProcessInstanceId(variable.getProcessInstanceId());
    copy.setName(variable.getName());
    copy.setValue(variable.getTypedValue());
    copy.setTaskId(variable.getTaskId());
    varValuesCache.put(variable.getId(), copy);    
  }
}

