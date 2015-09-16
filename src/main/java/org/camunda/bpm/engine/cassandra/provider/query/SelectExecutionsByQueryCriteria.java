package org.camunda.bpm.engine.cassandra.provider.query;

import static org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader.EXECUTIONS;
import static org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader.VARIABLES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.AbstractIndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExecutionIdByEventTypeAndNameIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExecutionIdByProcessIdIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExecutionIdByVariableValueIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexUtils;
import org.camunda.bpm.engine.cassandra.provider.indexes.ProcessIdByBusinessKeyIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.ProcessIdByProcessVariableValueIndex;
import org.camunda.bpm.engine.cassandra.provider.operation.EventSubscriptionOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.ExecutionEntityOperations;
import org.camunda.bpm.engine.cassandra.provider.operation.LoadedCompositeEntity;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader;
import org.camunda.bpm.engine.cassandra.provider.operation.VariableEntityOperations;
import org.camunda.bpm.engine.impl.EventSubscriptionQueryValue;
import org.camunda.bpm.engine.impl.ExecutionQueryImpl;
import org.camunda.bpm.engine.impl.QueryVariableValue;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

public class SelectExecutionsByQueryCriteria implements SelectListQueryHandler<ExecutionEntity, ExecutionQueryImpl> {

  @SuppressWarnings("unchecked")
  public List<ExecutionEntity> executeQuery(CassandraPersistenceSession session, ExecutionQueryImpl executionQuery) {
    Map<String,ExecutionEntity> resultMap = new HashMap<String, ExecutionEntity>();

    //get by execution id
    if(executionQuery.getExecutionId()!=null){
      ExecutionEntity entity=session.selectById(ExecutionEntity.class, executionQuery.getExecutionId());

      if(entity==null){
        return Collections.emptyList();
      }
      resultMap.put(entity.getId(), entity);
    }

    //convert business key to process instance id
    String queryProcessId = executionQuery.getProcessInstanceId();
    if(executionQuery.getBusinessKey() != null){
      queryProcessId = ExecutionEntityOperations.getIndexHandler(ProcessIdByBusinessKeyIndex.class)
          .getUniqueValue(null,session, executionQuery.getBusinessKey());
      if(executionQuery.getProcessInstanceId()!=null && queryProcessId != executionQuery.getProcessInstanceId()){
        //both business key and process instance id are specified and don't match
        return Collections.emptyList();
      }
    }
    
    //get / filter by process instance id
    if(queryProcessId!=null){
      if(!resultMap.isEmpty()){
        for (ExecutionEntity obj: resultMap.values()) {
          if(!obj.getProcessInstanceId().equals(queryProcessId)){
            //should only be one value at this point as we only looked by execution id
            return Collections.emptyList();
          }
        }
      }
      else{
        LoadedCompositeEntity loadedProcessInstance = session.selectCompositeById(ProcessInstanceLoader.NAME, queryProcessId);
        
        if(loadedProcessInstance != null) {
          resultMap=(Map<String, ExecutionEntity>) loadedProcessInstance.get(EXECUTIONS);
        }
        if(resultMap.isEmpty()){
          return Collections.emptyList();
        }
      }
    }
    
    if(!resultMap.isEmpty()){
      //filter by other query values
      return filter(resultMap, session, executionQuery);
    }
    
    //from here on cross-check the id sets from various queries before getting the entities
    
    Set<String> executionIdSet = null;

    // get by variables
    if(executionQuery.getQueryVariableValues() != null && !executionQuery.getQueryVariableValues().isEmpty()) {
      executionIdSet=getIdsByVariables(session, executionQuery.getQueryVariableValues(), executionIdSet);
      if(executionIdSet==null || executionIdSet.isEmpty()){
        return Collections.emptyList();
      }
    }

    // get by event subscriptions - only if we did not get anything by variables. 
    // If we did get something by variables we are assuming that the result set is small 
    // and it is better to filter by subscriptions instead of getting the whole subscription
    // index, which can be quite large
    
    if(executionIdSet==null || executionIdSet.isEmpty()){
      if(executionQuery.getEventSubscriptions() != null && !executionQuery.getEventSubscriptions().isEmpty()) {
        executionIdSet=getIdsByEventSubscriptions(session, executionQuery.getEventSubscriptions(), executionIdSet);
        if(executionIdSet==null || executionIdSet.isEmpty()){
          return Collections.emptyList();
        }
      }
    }

    if(executionIdSet==null || executionIdSet.isEmpty()){
      return Collections.emptyList();
    }
    
    //finally get the entities
    for(String id:executionIdSet){
      ExecutionEntity entity=session.selectById(ExecutionEntity.class, id);
      if(entity!=null){
        resultMap.put(id, entity);
      }
    }
    if(!resultMap.isEmpty()){
      //filter again in case some index inconsistency, also for events type for events with null names
      return filter(resultMap, session, executionQuery);
    }

    return new ArrayList<ExecutionEntity>(resultMap.values());
  }

  protected Set<String> getIdsByVariables(CassandraPersistenceSession session, List<QueryVariableValue> queryVariables, Set<String> executionIdSet){
    ExecutionIdByVariableValueIndex allExecutionsIndex = (ExecutionIdByVariableValueIndex) VariableEntityOperations.getIndexHandler(ExecutionIdByVariableValueIndex.class);

    for(QueryVariableValue queryVariableValue : queryVariables){
      if(queryVariableValue.isLocal()){
        List<String> queriedExecutionIds = allExecutionsIndex.getValuesByTypedValue(session, queryVariableValue.getName(), queryVariableValue.getTypedValue());
        if(queriedExecutionIds.isEmpty()){
          return null;
        }
        executionIdSet = executionIdSet==null ? new HashSet<String>(queriedExecutionIds) :
            IndexUtils.crossCheckIndexes(executionIdSet, new HashSet<String>(queriedExecutionIds));
      }
      else{
        executionIdSet = getIdsByProcessVariable(session, queryVariableValue, executionIdSet);        
      }
      if(executionIdSet.isEmpty()){
        return null;
      }
    }    
    return executionIdSet;
  }
  
  protected Set<String> getIdsByProcessVariable(CassandraPersistenceSession session, QueryVariableValue queryVariable, Set<String> executionIdSet){
    ProcessIdByProcessVariableValueIndex processIdIndex = (ProcessIdByProcessVariableValueIndex) VariableEntityOperations.getIndexHandler(ProcessIdByProcessVariableValueIndex.class);
    ExecutionIdByProcessIdIndex processExecutionsIndex = (ExecutionIdByProcessIdIndex) ExecutionEntityOperations.getIndexHandler(ExecutionIdByProcessIdIndex.class);
    List<String> queriedProcessIds=processIdIndex.getValuesByTypedValue(session, queryVariable.getName(), queryVariable.getTypedValue());
    for(String processId:queriedProcessIds){
      List<String> queriedExecutionIds = processExecutionsIndex.getValues(null,session, processId);
      if(queriedExecutionIds.isEmpty()){
        return Collections.emptySet();
      }
      executionIdSet = executionIdSet==null ? new HashSet<String>(queriedExecutionIds) :
        IndexUtils.crossCheckIndexes(executionIdSet, new HashSet<String>(queriedExecutionIds));
      if(executionIdSet.isEmpty()){
        return executionIdSet;
      }
    }
    return executionIdSet;
  }

  protected Set<String> getIdsByEventSubscriptions(CassandraPersistenceSession session, List<EventSubscriptionQueryValue> eventSubscriptions, Set<String> executionIdSet) {
    ExecutionIdByEventTypeAndNameIndex index = (ExecutionIdByEventTypeAndNameIndex) EventSubscriptionOperations.getIndexHandler(ExecutionIdByEventTypeAndNameIndex.class);

    for(EventSubscriptionQueryValue queryEvent : eventSubscriptions){
      if(queryEvent.getEventName()!=null) {
        List<String> queriedExecutionIds=index.getValues(null,session, queryEvent.getEventType(), queryEvent.getEventName());
        if(queriedExecutionIds.isEmpty()){
          return null;
        }
        executionIdSet = executionIdSet==null ? new HashSet<String>(queriedExecutionIds) :
          IndexUtils.crossCheckIndexes(executionIdSet, new HashSet<String>(queriedExecutionIds));
        if(executionIdSet.isEmpty()){
          return null;
        }
      }
    }    
    return executionIdSet;
  }

  /**
   * This method should filter the result map by everything in the query 
   * except process instance id, execution id and process business key
   * Right now only filters by variables and event subscriptions
   */
  protected List<ExecutionEntity> filter(Map<String, ExecutionEntity> resultMap, CassandraPersistenceSession session, ExecutionQueryImpl executionQuery){
    if(resultMap.isEmpty()){
      throw new IllegalArgumentException("Do not filter empty resultMap.");
    }
    if(executionQuery.getQueryVariableValues() != null && !executionQuery.getQueryVariableValues().isEmpty()) {
      filterByVariables(executionQuery.getQueryVariableValues(), resultMap, session);
    }
    if(executionQuery.getEventSubscriptions() != null && !executionQuery.getEventSubscriptions().isEmpty()) {
      filterByEventSubscriptions(executionQuery.getEventSubscriptions(), resultMap, session);
    }
    if(resultMap.isEmpty()){
      return Collections.emptyList();
    }
    else{
      return new ArrayList<ExecutionEntity>(resultMap.values());        
    }
  }
  
  protected void filterByEventSubscriptions(List<EventSubscriptionQueryValue> eventSubscriptions, Map<String, ExecutionEntity> resultMap, CassandraPersistenceSession session) {
    Set<String> filteredExecutionIds = new HashSet<String>();

    for(ExecutionEntity executionEntity : resultMap.values() ){
      for (EventSubscriptionQueryValue queriedEventSubscription : eventSubscriptions) {
        boolean found=false;
        for(EventSubscriptionEntity eventSubscriptionEntity : executionEntity.getEventSubscriptions()) {
          if(queriedEventSubscription.getEventType().equals(eventSubscriptionEntity.getEventType()) &&
              (queriedEventSubscription.getEventName()==null || queriedEventSubscription.getEventName().equals(eventSubscriptionEntity.getEventName()))) {
            found=true;
            break;
          }
        }
        if(!found){
          filteredExecutionIds.add(executionEntity.getId());
          break;
        }
      }
    }
    
    for (String filteredExecution : filteredExecutionIds) {
      resultMap.remove(filteredExecution);
    }
  }

  protected void filterByVariables(List<QueryVariableValue> variables, Map<String, ExecutionEntity> resultMap, CassandraPersistenceSession session) {
    Set<String> toRemove = new HashSet<String>();
    
    for(ExecutionEntity executionEntity:resultMap.values()){
      for(QueryVariableValue queryVariable:variables){
        Object var=null;
        if(queryVariable.isLocal()){
          var=executionEntity.getVariableLocal(queryVariable.getName());
        }
        else{
          LoadedCompositeEntity loadedProcessInstance = session.selectCompositeById(ProcessInstanceLoader.NAME, executionEntity.getProcessInstanceId());
          @SuppressWarnings("unchecked")
          Collection<VariableInstanceEntity> processVariables=(Collection<VariableInstanceEntity>) loadedProcessInstance.get(VARIABLES).values();            
          for(VariableInstanceEntity varEntity:processVariables){
            if(varEntity.getName().equals(queryVariable.getName())){
              var=varEntity.getValue();
            }
          }
        }
        if(var==null || !var.equals(queryVariable.getValue())){
          toRemove.add(executionEntity.getId());
          break;
        }
      }
    }
    
    for(String id:toRemove){
      resultMap.remove(id);
    }
  }
}
