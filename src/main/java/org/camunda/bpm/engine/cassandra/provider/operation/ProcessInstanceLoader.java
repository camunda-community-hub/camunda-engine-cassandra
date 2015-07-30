package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler.TABLE_NAME;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.ProcessInstanceBatch;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;

public class ProcessInstanceLoader implements CompositeEntityLoader {

  public static final String NAME = "process-instance-compostite";
  public static final String EVENT_SUBSCRIPTIONS = "event_subscriptions";
  public static final String EXECUTIONS = "executions";
  public static final String VARIABLES = "variables";

  public LoadedCompositeEntity getEntityById(CassandraPersistenceSession session, String id) {
    LoadedCompositeEntity loadedProcessInstance = new LoadedCompositeEntity();

    Session s = session.getSession();

    Row row = s.execute(select().all().from(TABLE_NAME).where(eq("id", id))).one();
    if(row == null) {
      return null;
    }

    int version = row.getInt("version");
    String businessKey = row.getString("business_key");

    CassandraSerializer<ExecutionEntity> executionSerializer = session.getSerializer(ExecutionEntity.class);
    CassandraSerializer<EventSubscriptionEntity> eventSubscriptionSerializer = session.getSerializer(EventSubscriptionEntity.class);
    CassandraSerializer<VariableInstanceEntity> variableSerializer = session.getSerializer(VariableInstanceEntity.class);

    // deserialize all executions
    Map<String, UDTValue> executionsMap = row.getMap(EXECUTIONS, String.class, UDTValue.class);
    Map<String, ExecutionEntity> executions = new HashMap<String, ExecutionEntity>();
    for (UDTValue serializedExecution : executionsMap.values()) {
      ExecutionEntity executionEntity = executionSerializer.read(serializedExecution);
      executions.put(executionEntity.getId(), executionEntity);
      if(executionEntity.isProcessInstanceExecution()) {
        loadedProcessInstance.setMainEntity(executionEntity);
      }

    }
    loadedProcessInstance.put(EXECUTIONS, executions);

    // deserialize all event subscription
    Map<String, UDTValue> eventSubscriptionsMap = row.getMap(EVENT_SUBSCRIPTIONS, String.class, UDTValue.class);
    Map<String, EventSubscriptionEntity> eventSubscriptions = new HashMap<String, EventSubscriptionEntity>();
    for (UDTValue serializedEventSubscription : eventSubscriptionsMap.values()) {
      EventSubscriptionEntity eventSubscriptionEntity = eventSubscriptionSerializer.read(serializedEventSubscription);
      eventSubscriptions.put(eventSubscriptionEntity.getId(), eventSubscriptionEntity);
    }
    loadedProcessInstance.put(EVENT_SUBSCRIPTIONS, eventSubscriptions);

    // deserialize all variables
    Map<String, UDTValue> variablesMap = row.getMap(VARIABLES, String.class, UDTValue.class);
    Map<String, VariableInstanceEntity> variables = new HashMap<String, VariableInstanceEntity>();
    for (UDTValue serializedVariable : variablesMap.values()) {
      VariableInstanceEntity variableEntity = variableSerializer.read(serializedVariable);
      variables.put(variableEntity.getId(), variableEntity);
    }
    loadedProcessInstance.put(VARIABLES, variables);

    reconstructEntityTree(loadedProcessInstance);

    ExecutionEntity processInstance= (ExecutionEntity) loadedProcessInstance.getPrimaryEntity();
    processInstance.setRevision(version);
    processInstance.setBusinessKey(businessKey);

    ProcessInstanceBatch batch = new ProcessInstanceBatch((ExecutionEntity) loadedProcessInstance.getPrimaryEntity());
    session.addLockedBatch(loadedProcessInstance.getPrimaryEntity().getId(), batch);

    return loadedProcessInstance;
  }

  @SuppressWarnings("unchecked")
  protected void reconstructEntityTree(LoadedCompositeEntity compositeEntity) {
    ExecutionEntity processInstance = (ExecutionEntity) compositeEntity.getPrimaryEntity();
    Map<String, ExecutionEntity> executions = (Map<String, ExecutionEntity>) compositeEntity.getEmbeddedEntities().get(EXECUTIONS);
    Map<String, EventSubscriptionEntity> eventSubscriptions = (Map<String, EventSubscriptionEntity>) compositeEntity.getEmbeddedEntities().get(EVENT_SUBSCRIPTIONS);
    Map<String, VariableInstanceEntity> variables = (Map<String, VariableInstanceEntity>) compositeEntity.getEmbeddedEntities().get(VARIABLES);

    processInstance.restoreProcessInstance(executions.values(),
        eventSubscriptions.values(),
        variables.values());
  }

}
