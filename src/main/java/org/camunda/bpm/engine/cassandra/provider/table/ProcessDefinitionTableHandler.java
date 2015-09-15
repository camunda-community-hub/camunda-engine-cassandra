package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.Session;

public class ProcessDefinitionTableHandler implements TableHandler {
  
  public final static String TABLE_NAME = "cam_proc_def";
  public final static String TABLE_NAME_IDX_VERSION = "cam_proc_def_idx_version";

  protected final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE_NAME +" "
      + "(id text, "
      + "key text, "
      + "version int, "
      + "category text, "
      + "name text, "
      + "deployment_id text, "
      + "suspension_state int, "
      + "PRIMARY KEY (id));";
  
  protected final static String CREATE_TABLE_IDX_VERSION = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME_IDX_VERSION +" "
      + "(key text, "
      + "version int, "
      + "id text, "
      + "PRIMARY KEY (key,version)) WITH CLUSTERING ORDER BY (version DESC);";
  
  protected final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME;
  
  protected final static String DROP_TABLE_IDX_VERSION = "DROP TABLE IF EXISTS "+TABLE_NAME_IDX_VERSION;

  protected final static String DEPLOYMENT_ID_IDX = "CREATE INDEX IF NOT EXISTS ON "  + TABLE_NAME + " ( deployment_id );";


  public List<String> getTableNames() {
    return Arrays.asList(TABLE_NAME, TABLE_NAME_IDX_VERSION);
  }

  public void createTable(Session s) {
    s.execute(CREATE_TABLE);
    s.execute(CREATE_TABLE_IDX_VERSION);
    s.execute(DEPLOYMENT_ID_IDX);
  }
  
  public void dropTable(Session s) {
    s.execute(DROP_TABLE_IDX_VERSION);
    s.execute(DROP_TABLE);
  }
}
