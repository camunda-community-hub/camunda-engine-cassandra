package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;


public interface CompositeEntityLoader {
  
  LoadedCompositeEntity getEntityById(CassandraPersistenceSession s, String id);  

}
