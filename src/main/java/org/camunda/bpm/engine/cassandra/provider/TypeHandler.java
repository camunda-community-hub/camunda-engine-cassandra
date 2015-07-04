package org.camunda.bpm.engine.cassandra.provider;

import com.datastax.driver.core.Session;

public interface TypeHandler<T> {
  
  void createType(Session s);
  
  void dropType(Session s);

}
