package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

public interface EntityOperationHandler<T extends DbEntity> {
  
  void insert(CassandraPersistenceSession session, T entity);

  void delete(CassandraPersistenceSession session, T entity);
  
  void update(CassandraPersistenceSession session, T entity);
  
  T getEntityById(CassandraPersistenceSession cassandraPersistenceSession, String id);  
}
