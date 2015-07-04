package org.camunda.bpm.engine.cassandra.provider.table;

import com.datastax.driver.core.Session;

public class ResourceTableHandler implements TableHandler {
  
  public final static String TABLE_NAME = "CAM_RESOURCE";

  protected final static String CREATE_TABLE_STMNT = "CREATE TABLE "+TABLE_NAME +" "
      + "(id text, "
      + "name text, "
      + "deployment_id text,"
      + "content blob, "
      + "PRIMARY KEY (id));";
  
  protected final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME;
  
  public void createTable(Session s) {
    s.execute(CREATE_TABLE_STMNT);
  }
  
  public void dropTable(Session s) {
    s.execute(DROP_TABLE);
  }
}
