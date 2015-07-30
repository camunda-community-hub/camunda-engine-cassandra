package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.Collections;
import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.type.EventSubscriptionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.ExecutionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.VariableTypeHandler;

import com.datastax.driver.core.Session;

public class ProcessInstanceTableHandler implements TableHandler {

  public final static String TABLE_NAME = "CAM_PROC_INST";

  protected final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE_NAME +" "
      + "(id text, "
      + "version int, "
      + "business_key text, "
      + "executions map <text, frozen <"+ExecutionTypeHandler.TYPE_NAME+">>,"
      + "event_subscriptions map <text, frozen <"+EventSubscriptionTypeHandler.TYPE_NAME+">>,"
      + "variables map <text, frozen <"+VariableTypeHandler.TYPE_NAME+">>,"
      + "PRIMARY KEY (id));";
  
  public final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME+";";
    
  public List<String> getTableNames() {
    return Collections.singletonList(TABLE_NAME);
  }

  public void createTable(Session s) {
    s.execute(CREATE_TABLE);
  }

  public void dropTable(Session s) {
    s.execute(DROP_TABLE);
  }
}
