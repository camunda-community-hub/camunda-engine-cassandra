package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExecutionIdByEventTypeAndNameIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.ProcessIdByEventSubscriptionIdIndex;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class EventSubscriptionOperations implements EntityOperationHandler<EventSubscriptionEntity> {
  protected static Map<Class<?>, IndexHandler<EventSubscriptionEntity>> indexHandlers = new HashMap<Class<?>, IndexHandler<EventSubscriptionEntity>>();
  static {
    indexHandlers.put(ExecutionIdByEventTypeAndNameIndex.class, new ExecutionIdByEventTypeAndNameIndex());
    indexHandlers.put(ProcessIdByEventSubscriptionIdIndex.class, new ProcessIdByEventSubscriptionIdIndex());
  }
  
  public void insert(CassandraPersistenceSession session, EventSubscriptionEntity entity) {
    session.addStatement(createUpdateStatement(session, entity));
    
    for(IndexHandler<EventSubscriptionEntity> index:indexHandlers.values()){
      session.addStatement(index.getInsertStatement(entity));    
    }
  }

  public void delete(CassandraPersistenceSession session, EventSubscriptionEntity entity) {    
    session.addStatement(QueryBuilder.delete().mapElt("event_subscriptions", entity.getId())
        .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId())),
        entity.getProcessInstanceId());
    
    for(IndexHandler<EventSubscriptionEntity> index:indexHandlers.values()){
      session.addIndexStatement(index.getDeleteStatement(entity), entity.getProcessInstanceId());    
    }
  }

  public void update(CassandraPersistenceSession session, EventSubscriptionEntity entity) {
    session.addStatement(createUpdateStatement(session, entity), entity.getProcessInstanceId());
    
    for(IndexHandler<EventSubscriptionEntity> index:indexHandlers.values()){
      session.addIndexStatement(index.getInsertStatement(entity), entity.getProcessInstanceId());    
    }
  }

  protected Statement createUpdateStatement(CassandraPersistenceSession session, EventSubscriptionEntity entity) {
    Session s = session.getSession();
    UDTypeHandler typeHander = session.getTypeHander(EventSubscriptionEntity.class);
    CassandraSerializer<EventSubscriptionEntity> serializer = session.getSerializer(EventSubscriptionEntity.class);
    
    UDTValue value = typeHander.createValue(s);
    serializer.write(value, entity);
    
    return QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(put("event_subscriptions", entity.getId(), value))
        .where(eq("id", entity.getProcessInstanceId()));
  }

  public EventSubscriptionEntity getEntityById(CassandraPersistenceSession session, String id) {
    String procId = indexHandlers.get(ProcessIdByEventSubscriptionIdIndex.class).getUniqueValue(session, id);
    if(procId==null){
      return null;
    }
    LoadedCompositeEntity loadedCompostite = session.selectCompositeById(ProcessInstanceLoader.NAME, procId);
    if(loadedCompostite==null){
      return null;
    }
    return (EventSubscriptionEntity) loadedCompostite.get(ProcessInstanceLoader.EVENT_SUBSCRIPTIONS).get(id);
  }
  
  public static IndexHandler<EventSubscriptionEntity> getIndexHandler(Class<?> type){
    return indexHandlers.get(type);
  }
}
