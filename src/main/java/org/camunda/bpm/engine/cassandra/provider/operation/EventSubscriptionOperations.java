package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class EventSubscriptionOperations implements ProcessSubentityOperationsHandler<EventSubscriptionEntity> {
  
  public void insert(CassandraPersistenceSession session, EventSubscriptionEntity entity) {
    session.addStatement(createUpdateStatement(session, entity));
  }

  public void delete(CassandraPersistenceSession session, EventSubscriptionEntity entity) {
    
    session.addStatement(QueryBuilder.delete().mapElt("event_subscriptions", entity.getId())
        .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId())),
        entity.getProcessInstanceId());
  }

  public void update(CassandraPersistenceSession session, EventSubscriptionEntity entity) {
    session.addStatement(createUpdateStatement(session, entity), entity.getProcessInstanceId());
  }

  protected Statement createUpdateStatement(CassandraPersistenceSession session, EventSubscriptionEntity entity) {
    Session s = session.getSession();
    UDTypeHandler typeHander = session.getTypeHander(EventSubscriptionEntity.class);
    CassandraSerializer<EventSubscriptionEntity> serializer = session.getSerializer(EventSubscriptionEntity.class);
    
    UDTValue value = typeHander.createValue(s);
    serializer.write(value, entity);
    
    return QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(put("event_subscriptions", entity.getId(), value))
        .where(eq("id", entity.getProcessInstanceId()));
  }

  /* (non-Javadoc)
   * @see org.camunda.bpm.engine.cassandra.provider.operation.ProcessSubentityOperationsHandler#getById(org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession, java.lang.String)
   */
  @Override
  public EventSubscriptionEntity getById(CassandraPersistenceSession session, String id) {
    // TODO Auto-generated method stub
    return null;
  }
  
}
