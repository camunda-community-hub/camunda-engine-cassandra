package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.BatchStatement;

public interface EntityOperationHandler<T extends DbEntity> {
  
  void insert(CassandraPersistenceSession session, T entity);

  void delete(CassandraPersistenceSession session, T entity);
  
  void update(CassandraPersistenceSession session, T entity);
  
}
