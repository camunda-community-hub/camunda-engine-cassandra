package org.camunda.bpm.engine.cassandra.provider.query;

import static org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader.EVENT_SUBSCRIPTIONS;
import static org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader.EXECUTIONS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExecutionIdByEventTypeAndNameIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.ProcessIdByBusinessKeyIndex;
import org.camunda.bpm.engine.cassandra.provider.operation.EventSubscriptionOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.ExecutionEntityOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.LoadedCompositeEntity;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader;
import org.camunda.bpm.engine.impl.EventSubscriptionQueryValue;
import org.camunda.bpm.engine.impl.ExecutionQueryImpl;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

public class SelectExecutionsByQueryCriteria implements SelectListQueryHandler<ExecutionEntity, ExecutionQueryImpl> {

  @SuppressWarnings("unchecked")
  public List<ExecutionEntity> executeQuery(CassandraPersistenceSession session, ExecutionQueryImpl executionQuery) {
    List<EventSubscriptionQueryValue> eventSubscriptions = executionQuery.getEventSubscriptions();
    
    if(executionQuery.getProcessInstanceId() == null && executionQuery.getExecutionId()==null && (eventSubscriptions == null || eventSubscriptions.isEmpty())) {
      throw new RuntimeException("Unsupported Execution Query: only queries by process instance id, execution id or event subscriptions are supported");
    }
    
    Map<String,ExecutionEntity> resultMap = new HashMap<String, ExecutionEntity>();
    String processId = null;

    //get by execution id
    if(executionQuery.getExecutionId()!=null){
      ExecutionEntity entity=session.selectById(ExecutionEntity.class, executionQuery.getExecutionId());

      if(entity==null){
        return new ArrayList<ExecutionEntity>();
      }
      resultMap.put(entity.getId(), entity);
      processId=entity.getProcessInstanceId();
    }

    String queryProcessId = executionQuery.getProcessInstanceId();
    if(executionQuery.getBusinessKey() != null){
      queryProcessId = ExecutionEntityOperations.getIndexHandler(ProcessIdByBusinessKeyIndex.class)
          .getUniqueValue(session, executionQuery.getBusinessKey());
      if(executionQuery.getProcessInstanceId()!=null && queryProcessId != executionQuery.getProcessInstanceId()){
        //both business key and process instance id are specified and don't match
        return new ArrayList<ExecutionEntity>();
      }
    }
    
    //get / filter by process instance id
    if(queryProcessId!=null){
      if(!resultMap.isEmpty()){
        for (ExecutionEntity obj: resultMap.values()) {
          if(!obj.getProcessInstanceId().equals(queryProcessId)){
            //should only be one value at this point as we only looked by execution id
            return new ArrayList<ExecutionEntity>();
          }
        }
       }
      else{
        LoadedCompositeEntity loadedProcessInstance = session.selectCompositeById(ProcessInstanceLoader.NAME, queryProcessId);
        
        if(loadedProcessInstance != null) {
          resultMap=(Map<String, ExecutionEntity>) loadedProcessInstance.get(EXECUTIONS);
          if(resultMap.isEmpty()){
            return new ArrayList<ExecutionEntity>();
          }
        }
      }
      processId=queryProcessId;
   }
    
    // get / filter by event subscription
    if(eventSubscriptions != null && !eventSubscriptions.isEmpty()) {
      if(!resultMap.isEmpty()){
        LoadedCompositeEntity loadedProcessInstance = session.selectCompositeById(ProcessInstanceLoader.NAME, processId);
        filterByEventSubscriptions(eventSubscriptions, resultMap, loadedProcessInstance);
        if(resultMap.isEmpty()){
          return new ArrayList<ExecutionEntity>();
        }
      }
      else{
        getByEventSubscriptions(session, eventSubscriptions, resultMap);
      }
    }
    
    return new ArrayList<ExecutionEntity>(resultMap.values());
  }

  protected void getByEventSubscriptions(CassandraPersistenceSession session, List<EventSubscriptionQueryValue> eventSubscriptions, Map<String, ExecutionEntity> resultMap) {
    IndexHandler<EventSubscriptionEntity> index = EventSubscriptionOperations.getIndexHandler(ExecutionIdByEventTypeAndNameIndex.class);
    List<Set<String>> executionIdSets = new ArrayList<Set<String>>();
    for(EventSubscriptionQueryValue eventSubscription : eventSubscriptions){
      HashSet<String> executionIdSet = new HashSet<String>();
      executionIdSet.addAll(index.getValues(session, eventSubscription.getEventType(), eventSubscription.getEventName()));
      if(executionIdSet.isEmpty()){
        resultMap.clear();
        return;
      }
      executionIdSets.add(executionIdSet);        
    }    
    
    Set<String> executionIds=index.crossCheckIndexes(executionIdSets);
    
    for(String executionId:executionIds){
      ExecutionEntity entity=session.selectById(ExecutionEntity.class, executionId);
      if(entity!=null){
        resultMap.put(entity.getId(), entity);
      }
    }    
  }

  protected void filterByEventSubscriptions(List<EventSubscriptionQueryValue> eventSubscriptions, Map<String, ExecutionEntity> resultMap, LoadedCompositeEntity loadedProcessInstance) {
    Set<String> filteredExecutionIds = new HashSet<String>(resultMap.keySet());

    for (DbEntity obj: loadedProcessInstance.get(EVENT_SUBSCRIPTIONS).values()) {
      EventSubscriptionEntity eventSubscriptionEntity = (EventSubscriptionEntity) obj;
      for (EventSubscriptionQueryValue queriedEventSubscription : eventSubscriptions) {
        if(queriedEventSubscription.getEventType().equals(eventSubscriptionEntity.getEventType()) &&
            (queriedEventSubscription.getEventName()==null || queriedEventSubscription.getEventName().equals(eventSubscriptionEntity.getEventName()))) {
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
