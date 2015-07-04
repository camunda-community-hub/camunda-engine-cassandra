package org.camunda.bpm.engine.cassandra.provider.serializer;

import java.util.Date;

import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.MessageEventSubscriptionEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

public class EventSubscriptionSerializer implements CassandraSerializer<EventSubscriptionEntity> {

  public void write(SettableData<?> data, EventSubscriptionEntity entity) {
    data.setString("id", entity.getId())
    .setString("event_type", entity.getEventType())
    .setString("event_name", entity.getEventName())
    .setString("execution_id", entity.getExecutionId())
    .setString("proc_inst_id", entity.getProcessInstanceId())
    .setString("activity_id", entity.getActivityId())
    .setString("configuration", entity.getConfiguration())
    .setLong("created", entity.getCreated().getTime());    
  }

  public EventSubscriptionEntity read(GettableData data) {
    String eventType = data.getString("event_type");
    if("message".equals(eventType)) {
      MessageEventSubscriptionEntity entity = new MessageEventSubscriptionEntity();
      entity.setId(data.getString("id"));
      entity.setEventType(data.getString("event_type"));
      entity.setEventName(data.getString("event_name"));
      entity.setExecutionId(data.getString("execution_id"));
      entity.setProcessInstanceId(data.getString("proc_inst_id"));
      entity.setActivityId(data.getString("activity_id"));
      entity.setConfiguration(data.getString("configuration"));
      entity.setCreated(new Date(data.getLong("created")));
      return entity;
    }
    // TODO: other types!
    throw new RuntimeException("Unsupported type '"+eventType+"'.");
  }

}
