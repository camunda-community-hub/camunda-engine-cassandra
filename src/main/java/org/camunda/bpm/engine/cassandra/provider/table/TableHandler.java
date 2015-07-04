package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.List;

import com.datastax.driver.core.Session;

public interface TableHandler {

  List<String> getTableNames();

  void createTable(Session s);
  
  void dropTable(Session s);

}
