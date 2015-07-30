package org.camunda.bpm.engine.cassandra.provider.indexes;

import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public class ProcessIdByExecutionIdIndex extends AbstractIndexHandler<ExecutionEntity> {

  @Override
  protected String getIndexName() {
    return IndexNames.PROCESS_ID_BY_EXECUTION_ID;
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  protected String getIndexValue(ExecutionEntity entity) {
    return entity.getId();
  }

  @Override
  protected String getValue(ExecutionEntity entity) {
    return entity.getProcessInstanceId();
  }

}
