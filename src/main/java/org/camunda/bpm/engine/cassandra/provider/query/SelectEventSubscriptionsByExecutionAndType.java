package org.camunda.bpm.engine.cassandra.provider.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
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
    List<EventSubscriptionEntity> result=new ArrayList<EventSubscriptionEntity>();
    ExecutionEntity entity=session.selectById(ExecutionEntity.class, map.get("executionId"));
    if(entity==null){
      return result;
    }
    for(EventSubscriptionEntity eventSubscriptionEntity:entity.getEventSubscriptions()){
      if(eventSubscriptionEntity.getEventType().equals(map.get("eventType"))){
        result.add(eventSubscriptionEntity);
      }
    }
    return result;
  }

}
