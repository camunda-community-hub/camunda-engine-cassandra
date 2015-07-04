package org.camunda.bpm.engine.cassandra.provider.type;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;

public interface UDTypeHandler {
  
  String getTypeName();
  
  void createType(Session s);
  
  void dropType(Session s);
  
  UDTValue createValue(Session s);

}
