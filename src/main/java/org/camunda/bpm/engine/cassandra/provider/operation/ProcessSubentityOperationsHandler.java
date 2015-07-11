package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public interface ProcessSubentityOperationsHandler<T extends DbEntity> extends EntityOperationHandler<T> {
  T getById(CassandraPersistenceSession session, String id); 
}
