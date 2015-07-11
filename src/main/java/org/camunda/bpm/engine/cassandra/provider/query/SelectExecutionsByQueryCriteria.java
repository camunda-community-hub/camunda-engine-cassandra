package org.camunda.bpm.engine.cassandra.provider.query;

import static org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader.EVENT_SUBSCRIPTIONS;
import static org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader.EXECUTIONS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.operation.LoadedCompositeEntity;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader;
import org.camunda.bpm.engine.impl.EventSubscriptionQueryValue;
import org.camunda.bpm.engine.impl.ExecutionQueryImpl;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

public class SelectExecutionsByQueryCriteria implements SelectListQueryHandler<ExecutionEntity, ExecutionQueryImpl> {

  public List<ExecutionEntity> executeQuery(CassandraPersistenceSession session, ExecutionQueryImpl executionQuery) {
    
    if(executionQuery.getProcessInstanceId() == null && executionQuery.getExecutionId()==null) {
      throw new RuntimeException("Unsupported Execution Query: process instance id or execution id needs to be provided.");
    }
    if(executionQuery.getExecutionId()!=null){
      return Arrays.asList(session.selectById(ExecutionEntity.class, executionQuery.getExecutionId()));
    }
    LoadedCompositeEntity loadedProcessInstance = session.selectCompositeById(ProcessInstanceLoader.NAME, executionQuery.getProcessInstanceId());
    
    if(loadedProcessInstance == null) {
      return null;
    }
    
    Map<String, ExecutionEntity> resultMap = (Map) loadedProcessInstance.get(EXECUTIONS);
    
    // filter by event subscription
    List<EventSubscriptionQueryValue> eventSubscriptions = executionQuery.getEventSubscriptions();
    if(eventSubscriptions != null && !eventSubscriptions.isEmpty()) {
      filterByEventSubscriptions(eventSubscriptions, resultMap, loadedProcessInstance);
    }
    
    return new ArrayList<ExecutionEntity>(resultMap.values());
  }

  protected void filterByEventSubscriptions(List<EventSubscriptionQueryValue> eventSubscriptions, Map<String, ExecutionEntity> resultMap, LoadedCompositeEntity loadedProcessInstance) {
    Set<String> filteredExecutionIds = new HashSet<String>(resultMap.keySet());

    for (DbEntity obj: loadedProcessInstance.get(EVENT_SUBSCRIPTIONS).values()) {
      EventSubscriptionEntity eventSubscriptionEntity = (EventSubscriptionEntity) obj;
      for (EventSubscriptionQueryValue queriedEventSubscription : eventSubscriptions) {
        if(queriedEventSubscription.getEventType().equals(eventSubscriptionEntity.getEventType()) &&
            queriedEventSubscription.getEventName().equals(eventSubscriptionEntity.getEventName())) {
          filteredExecutionIds.remove(eventSubscriptionEntity.getExecutionId());
          break;
        }
        
      }
    }
    
    for (String filteredExecution : filteredExecutionIds) {
      resultMap.remove(filteredExecution);
    }
  }

}
