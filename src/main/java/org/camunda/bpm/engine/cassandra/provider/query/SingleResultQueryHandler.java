package org.camunda.bpm.engine.cassandra.provider.query;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

public interface SingleResultQueryHandler<T extends DbEntity> {

  T executeQuery(CassandraPersistenceSession session, Object parameter);
  
}
