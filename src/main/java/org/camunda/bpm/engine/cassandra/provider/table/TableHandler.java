package org.camunda.bpm.engine.cassandra.provider.table;

import com.datastax.driver.core.Session;

public interface TableHandler {
  
  void createTable(Session s);
  
  void dropTable(Session s);

}
