package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;


public interface SingleEntityLoader<T extends DbEntity> {
  
  T getEntityById(CassandraPersistenceSession cassandraPersistenceSession, String id);  

}
