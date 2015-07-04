package org.camunda.bpm.engine.cassandra.provider.type;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;

public abstract class AbstractTypeHandler implements UDTypeHandler {

  public void createType(Session s) {
    s.execute(getCreateStatement());
  }

  public void dropType(Session s) {
    s.execute(getDropStatement());
  }
  
  protected abstract String getCreateStatement();
  
  protected abstract String getDropStatement();

  public UDTValue createValue(Session s) {
    return s.getCluster()
        .getMetadata()
        .getKeyspace(s.getLoggedKeyspace())
        .getUserType(getTypeName())
        .newValue();
  }
  
}
