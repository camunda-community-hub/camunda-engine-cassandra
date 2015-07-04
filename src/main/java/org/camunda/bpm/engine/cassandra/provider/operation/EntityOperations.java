package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.BatchStatement;

public interface EntityOperations<T extends DbEntity> {
  
  void insert(CassandraPersistenceSession session, T entity, BatchStatement flush);

  void delete(CassandraPersistenceSession session, T entity, BatchStatement flush);
  
  void update(CassandraPersistenceSession session, T entity, BatchStatement flush);
  
}
