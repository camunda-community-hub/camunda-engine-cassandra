package org.camunda.bpm.engine.cassandra.provider.query;

import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

public interface SelectListQueryHandler<T extends DbEntity, Q> {
  
  List<T> executeQuery(CassandraPersistenceSession session, Q parameter);

}
