package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;

import com.datastax.driver.core.BatchStatement;

public interface BulkOperationHandler {
  
  void perform(CassandraPersistenceSession session, Object parameter, BatchStatement flush);

}
