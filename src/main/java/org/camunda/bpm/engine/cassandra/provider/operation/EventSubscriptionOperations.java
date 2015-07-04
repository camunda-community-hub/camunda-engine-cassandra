package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class EventSubscriptionOperations implements EntityOperations<EventSubscriptionEntity> {
  
  public void insert(CassandraPersistenceSession session, EventSubscriptionEntity entity, BatchStatement flush) {
    
    Session s = session.getSession();
    UDTypeHandler typeHander = session.getTypeHander(EventSubscriptionEntity.class);
    CassandraSerializer<EventSubscriptionEntity> serializer = session.getSerializer(EventSubscriptionEntity.class);
    
    UDTValue value = typeHander.createValue(s);
    serializer.write(value, entity);
    
    flush.add(QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(put("event_subscriptions", entity.getId(), value))
        .where(eq("id", entity.getProcessInstanceId())));
  }

  public void delete(CassandraPersistenceSession session, EventSubscriptionEntity entity, BatchStatement flush) {
    // TODO Auto-generated method stub
    
  }

  public void update(CassandraPersistenceSession session, EventSubscriptionEntity entity, BatchStatement flush) {
    // TODO Auto-generated method stub
    
  }
  
  
  
}
