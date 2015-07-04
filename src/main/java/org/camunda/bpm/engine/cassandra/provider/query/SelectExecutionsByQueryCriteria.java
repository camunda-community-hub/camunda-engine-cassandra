package org.camunda.bpm.engine.cassandra.provider.query;

import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.ExecutionQueryImpl;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

public class SelectExecutionsByQueryCriteria implements SelectListQueryHandler<ExecutionEntity, ExecutionQueryImpl> {

  public List<ExecutionEntity> executeQuery(CassandraPersistenceSession session, ExecutionQueryImpl parameter) {
    return null;
  }

}
