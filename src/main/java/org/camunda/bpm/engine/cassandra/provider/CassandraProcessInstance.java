package org.camunda.bpm.engine.cassandra.provider;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

public class CassandraProcessInstance {

  protected Map<String, ExecutionEntity> executions = new HashMap<String, ExecutionEntity>();
  protected Map<String, EventSubscriptionEntity> eventSubscriptions = new HashMap<String, EventSubscriptionEntity>();

  public Map<String, ExecutionEntity> getExecutions() {
    return executions;
  }
  public void setExecutions(Map<String, ExecutionEntity> executions) {
    this.executions = executions;
  }
  public Map<String, EventSubscriptionEntity> getEventSubscriptions() {
    return eventSubscriptions;
  }
  public void setEventSubscriptions(Map<String, EventSubscriptionEntity> eventSubscriptions) {
    this.eventSubscriptions = eventSubscriptions;
  }
  
}
