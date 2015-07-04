package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler.TABLE_NAME;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;

public class ProcessInstanceLoader implements CompositeEntityLoader {

  public LoadedCompositeEntity getEntityById(CassandraPersistenceSession session, String id) {
    LoadedCompositeEntity loadedProcessInstance = new LoadedCompositeEntity();
    
    Session s = session.getSession();
    
    Row row = s.execute(select().all().from(TABLE_NAME).where(eq("id", id))).one();
    if(row == null) {
      return null;
    }

    CassandraSerializer<ExecutionEntity> executionSerializer = session.getSerializer(ExecutionEntity.class);
    CassandraSerializer<EventSubscriptionEntity> eventSubscriptionSerializer = session.getSerializer(EventSubscriptionEntity.class);
    CassandraSerializer<VariableInstanceEntity> variableSerializer = session.getSerializer(VariableInstanceEntity.class);
    
    // deserialize all executions
    Map<String, UDTValue> executionsMap = row.getMap("executions", String.class, UDTValue.class);
    Map<String, ExecutionEntity> executions = new HashMap<String, ExecutionEntity>();
    for (UDTValue serializedExecution : executionsMap.values()) {
      ExecutionEntity executionEntity = executionSerializer.read(serializedExecution);
      executions.put(executionEntity.getId(), executionEntity);
      if(executionEntity.isProcessInstanceExecution()) {
        loadedProcessInstance.setMainEntity(executionEntity);
      }
      
    }
    loadedProcessInstance.put("executions", executions);
    
    // deserialize all event subscription    
    Map<String, UDTValue> eventSubscriptionsMap = row.getMap("event_subscriptions", String.class, UDTValue.class);
    Map<String, EventSubscriptionEntity> eventSubscriptions = new HashMap<String, EventSubscriptionEntity>();
    for (UDTValue serializedEventSubscription : eventSubscriptionsMap.values()) {
      EventSubscriptionEntity eventSubscriptionEntity = eventSubscriptionSerializer.read(serializedEventSubscription);
      eventSubscriptions.put(eventSubscriptionEntity.getId(), eventSubscriptionEntity);
    }
    loadedProcessInstance.put("eventSubscriptions", eventSubscriptions);
    
    // deserialize all variables    
    Map<String, UDTValue> variablesMap = row.getMap("variables", String.class, UDTValue.class);
    Map<String, VariableInstanceEntity> variables = new HashMap<String, VariableInstanceEntity>();
    for (UDTValue serializedVariable : variablesMap.values()) {
      VariableInstanceEntity variableEntity = variableSerializer.read(serializedVariable);
      variables.put(variableEntity.getId(), variableEntity);
    }
    loadedProcessInstance.put("variables", variables);
    
    return loadedProcessInstance;
  }

}
