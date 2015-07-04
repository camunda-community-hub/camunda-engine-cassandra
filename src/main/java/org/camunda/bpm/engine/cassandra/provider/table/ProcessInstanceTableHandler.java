package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.type.EventSubscriptionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.ExecutionTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;

public class ProcessInstanceTableHandler implements TableHandler {

  public final static String TABLE_NAME = "CAM_PROC_INST";

  protected final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE_NAME +" "
      + "(id text, "
      + "version int, "
      + "business_key text, "
      + "executions map <text, frozen <"+ExecutionTypeHandler.TYPE_NAME+">>,"
      + "event_subscriptions map <text, frozen <"+EventSubscriptionTypeHandler.TYPE_NAME+">>,"
      + "PRIMARY KEY (id));";
  
  public final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME+";";
  
  public void createTable(Session s) {
    s.execute(CREATE_TABLE);
  }

  public void dropTable(Session s) {
    s.execute(DROP_TABLE);
  }
//
//  public List<? extends Statement> createInsertStatement(Session s, ExecutionEntity entity) {

//  }
//
//  public CassandraProcessInstance findById(Session s, String processInstanceId) {
//    Row row = s.execute(select().all().from(TABLE_NAME).where(eq("id", processInstanceId))).one();
//    if(row == null) {
//      return null;
//    }
//    
//    ExecutionTypeHandler executionTypeHandler = new ExecutionTypeHandler();
//    EventSubscriptionTypeHandler eventSubscriptionTypeHandler = new EventSubscriptionTypeHandler();
//    
//    CassandraProcessInstance cassandraProcessInstance = new CassandraProcessInstance();
//    
//    // deserialize all executions
//    Map<String, UDTValue> executionsMap = row.getMap("executions", String.class, UDTValue.class);
//    for (UDTValue serializedExecution : executionsMap.values()) {
//      ExecutionEntity executionEntity = executionTypeHandler.deserializeValue(serializedExecution);
//      cassandraProcessInstance.getExecutions().put(executionEntity.getId(), executionEntity);
//    }
//    
//    // deserialize all event subscription    
//    Map<String, UDTValue> eventSubscriptionsMap = row.getMap("event_subscriptions", String.class, UDTValue.class);
//    for (UDTValue serializedEventSubscription : eventSubscriptionsMap.values()) {
//      EventSubscriptionEntity eventSubscriptionEntity = eventSubscriptionTypeHandler.deserializeValue(serializedEventSubscription);
//      cassandraProcessInstance.getEventSubscriptions().put(eventSubscriptionEntity.getId(), eventSubscriptionEntity);
//    }
//    return cassandraProcessInstance;
//  }
  
}
