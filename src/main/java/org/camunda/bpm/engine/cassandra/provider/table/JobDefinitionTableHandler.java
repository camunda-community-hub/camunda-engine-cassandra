package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.Session;

public class JobDefinitionTableHandler implements TableHandler {
  
  public final static String TABLE_NAME = "cam_job_def";

  protected final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE_NAME +" "
      + "(id text, "
      + "proc_def_id text, "
      + "proc_def_key text, "
      + "act_id text, "
      + "type text, "
      + "config text, "
      + "priority int, "
      + "suspension_state int, "
      + "revision int, "
      + "PRIMARY KEY (id));";
  
  protected final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME;
  
  public List<String> getTableNames() {
    return Arrays.asList(TABLE_NAME);
  }

  public void createTable(Session s) {
    s.execute(CREATE_TABLE);
  }
  
  public void dropTable(Session s) {
    s.execute(DROP_TABLE);
  }
}
