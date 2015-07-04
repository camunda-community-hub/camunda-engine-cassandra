package org.camunda.bpm.engine.cassandra.provider.table;

import com.datastax.driver.core.Session;

public abstract class AbstractTableHandler implements TableHandler {

  public void createTable(Session s) {
    s.execute(getCreateStatement());
  }

  public void dropTable(Session s) {
    s.execute(getDropStatement());
  }
  
  protected abstract String getDropStatement();

  protected abstract String getCreateStatement();

}
