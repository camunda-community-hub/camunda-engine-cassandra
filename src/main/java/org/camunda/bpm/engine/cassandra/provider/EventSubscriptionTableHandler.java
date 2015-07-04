package org.camunda.bpm.engine.cassandra.provider;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.util.ArrayList;
import java.util.List;

import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class EventSubscriptionTableHandler implements TableHandler<EventSubscriptionEntity> {

  public void createTable(Session s) {

  }

  public void dropTable(Session s) {

  }

  public List<? extends Statement> createInsertStatement(Session s, EventSubscriptionEntity entity) {   
    List<Statement> statements = new ArrayList<Statement>();
    statements.add(update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(put("event_subscriptions", entity.getId(), new EventSubscriptionTypeHandler().createValue(s, entity))).where(eq("id", entity.getProcessInstanceId())));
    return statements;
  }

}
