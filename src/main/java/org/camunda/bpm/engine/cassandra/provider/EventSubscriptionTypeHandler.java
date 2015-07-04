package org.camunda.bpm.engine.cassandra.provider;

import java.util.Date;

import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.MessageEventSubscriptionEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

public class EventSubscriptionTypeHandler implements TypeHandler<EventSubscriptionEntity> {

  public final static String TYPE_NAME = "event_subscription";
  
  public final static String CREATE = "CREATE TYPE IF NOT EXISTS "+TYPE_NAME+" ("
      + "id text, "
      + "event_type text, "
      + "event_name text, "
      + "execution_id text, "
      + "proc_inst_id text, "
      + "activity_id text, "
      + "configuration text, "
      + "created bigint, "
      + ");";
  
  public final static String DROP = "DROP TYPE IF EXISTS "+TYPE_NAME+";";
  
  public void createType(Session s) {
    s.execute(CREATE);
  }

  public void dropType(Session s) {
    s.execute(DROP);
  }

  public UDTValue createValue(Session s, EventSubscriptionEntity entity) {
    UserType userType = s.getCluster().getMetadata().getKeyspace(s.getLoggedKeyspace())
        .getUserType(TYPE_NAME);
       
    return userType.newValue()
      .setString("id", entity.getId())
      .setString("event_type", entity.getEventType())
      .setString("event_name", entity.getEventName())
      .setString("execution_id", entity.getExecutionId())
      .setString("proc_inst_id", entity.getProcessInstanceId())
      .setString("activity_id", entity.getActivityId())
      .setString("configuration", entity.getConfiguration())
      .setLong("created", entity.getCreated().getTime());
  }

  public EventSubscriptionEntity deserializeValue(UDTValue serializedEventSubscription) {
    String eventType = serializedEventSubscription.getString("event_type");
    if("message".equals(eventType)) {
      MessageEventSubscriptionEntity entity = new MessageEventSubscriptionEntity();
      entity.setId(serializedEventSubscription.getString("id"));
      entity.setEventType(serializedEventSubscription.getString("event_type"));
      entity.setEventName(serializedEventSubscription.getString("event_name"));
      entity.setExecutionId(serializedEventSubscription.getString("execution_id"));
      entity.setProcessInstanceId(serializedEventSubscription.getString("proc_inst_id"));
      entity.setActivityId(serializedEventSubscription.getString("activity_id"));
      entity.setConfiguration(serializedEventSubscription.getString("configuration"));
      entity.setCreated(new Date(serializedEventSubscription.getLong("created")));
      return entity;
    }
    // TODO: other types!
    throw new RuntimeException("Unsupported type '"+eventType+"'.");
  }

}
