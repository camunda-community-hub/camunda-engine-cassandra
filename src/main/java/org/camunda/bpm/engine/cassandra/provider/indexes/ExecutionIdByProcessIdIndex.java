package org.camunda.bpm.engine.cassandra.provider.indexes;

import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public class ExecutionIdByProcessIdIndex extends AbstractIndexHandler<ExecutionEntity> {

  @Override
  protected String getIndexName() {
    return IndexNames.EXECUTION_ID_BY_PROCESS_ID;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  protected String getIndexValue(ExecutionEntity entity) {
    return entity.getProcessInstanceId();
  }

  @Override
  protected String getValue(ExecutionEntity entity) {
    return entity.getId();
  }

}
