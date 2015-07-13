package org.camunda.bpm.engine.cassandra.provider.query;

import static org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader.EVENT_SUBSCRIPTIONS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.operation.LoadedCompositeEntity;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.ListQueryParameterObject;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

/**
 * @author Natalia Levine
 *
 * @created 14/07/2015
 */
public class SelectEventSubscriptionsByExecutionAndType implements SelectListQueryHandler<EventSubscriptionEntity, ListQueryParameterObject>{

  @Override
  public List<EventSubscriptionEntity> executeQuery(CassandraPersistenceSession session, ListQueryParameterObject parameter) {
    @SuppressWarnings("unchecked")
    Map<String, String> map=((Map<String, String>)parameter.getParameter());
    ExecutionEntity entity=session.selectById(ExecutionEntity.class, map.get("executionId"));
    LoadedCompositeEntity loadedProcessInstance = session.selectCompositeById(ProcessInstanceLoader.NAME, entity.getProcessInstanceId());
    List<EventSubscriptionEntity> result=new ArrayList<EventSubscriptionEntity>();
    for(DbEntity dbEntity:loadedProcessInstance.get(EVENT_SUBSCRIPTIONS).values()){
      EventSubscriptionEntity eventSubscriptionEntity = (EventSubscriptionEntity) dbEntity;
      if(eventSubscriptionEntity.getExecutionId().equals(map.get("executionId")) &&
          eventSubscriptionEntity.getEventType().equals(map.get("eventType"))){
        result.add(eventSubscriptionEntity);
      }
    }
    return result;
  }

}
