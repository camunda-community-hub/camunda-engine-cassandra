package org.camunda.bpm.engine.cassandra.provider;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;

public class CassandraProcessInstance {

  protected Map<String, DbEntity> executions = new HashMap<String, DbEntity>();
  protected Map<String, EventSubscriptionEntity> eventSubscriptions = new HashMap<String, EventSubscriptionEntity>();

  public Map<String, DbEntity> getExecutions() {
    return executions;
  }
  public void setExecutions(Map<String, DbEntity> executions) {
    this.executions = executions;
  }
  public Map<String, EventSubscriptionEntity> getEventSubscriptions() {
    return eventSubscriptions;
  }
  public void setEventSubscriptions(Map<String, EventSubscriptionEntity> eventSubscriptions) {
    this.eventSubscriptions = eventSubscriptions;
  }
  
}
