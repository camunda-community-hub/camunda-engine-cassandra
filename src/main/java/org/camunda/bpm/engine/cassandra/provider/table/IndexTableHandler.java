package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.Session;

public class IndexTableHandler implements TableHandler {

  public final static String INDEX_TABLE_NAME = "CAM_INDEX";

  protected final static String CREATE_INDEX_TABLE = "CREATE TABLE IF NOT EXISTS "+INDEX_TABLE_NAME +" "
      + "(idx_name text, "
      + "idx_value text, "
      + "val text, "
      + "PRIMARY KEY ((idx_name, idx_value), val));";
  
  public final static String DROP_INDEX_TABLE = "DROP TABLE IF EXISTS "+INDEX_TABLE_NAME+";";
    
  public List<String> getTableNames() {
    return Collections.singletonList(INDEX_TABLE_NAME);
  }

  public void createTable(Session s) {
    s.execute(CREATE_INDEX_TABLE);
  }

  public void dropTable(Session s) {
    s.execute(DROP_INDEX_TABLE);
  }
}
