package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.Arrays;
import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.type.EventSubscriptionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.ExecutionTypeHandler;
import org.camunda.bpm.engine.cassandra.provider.type.VariableTypeHandler;

import com.datastax.driver.core.Session;

public class ProcessInstanceTableHandler implements TableHandler {

  public final static String TABLE_NAME = "CAM_PROC_INST";
  public final static String INDEX_TABLE_NAME = "CAM_PROC_INST_IDX";

  protected final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE_NAME +" "
      + "(id text, "
      + "version int, "
      + "business_key text, "
      + "executions map <text, frozen <"+ExecutionTypeHandler.TYPE_NAME+">>,"
      + "event_subscriptions map <text, frozen <"+EventSubscriptionTypeHandler.TYPE_NAME+">>,"
      + "variables map <text, frozen <"+VariableTypeHandler.TYPE_NAME+">>,"
      + "PRIMARY KEY (id));";
  
  protected final static String CREATE_INDEX_TABLE = "CREATE TABLE IF NOT EXISTS "+INDEX_TABLE_NAME +" "
      + "(idx_name text, "
      + "idx_value text, "
      + "val text, "
      + "PRIMARY KEY ((idx_name, idx_value), val));";
  
  public final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME+";";
  public final static String DROP_INDEX_TABLE = "DROP TABLE IF EXISTS "+INDEX_TABLE_NAME+";";
    
  public List<String> getTableNames() {
    return Arrays.asList(TABLE_NAME, INDEX_TABLE_NAME);
  }

  public void createTable(Session s) {
    s.execute(CREATE_TABLE);
    s.execute(CREATE_INDEX_TABLE);
  }

  public void dropTable(Session s) {
    s.execute(DROP_TABLE);
    s.execute(DROP_INDEX_TABLE);
  }
}
