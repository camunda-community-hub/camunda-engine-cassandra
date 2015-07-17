package org.camunda.bpm.engine.cassandra.provider.indexes;

import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

/**
 * @author Natalia Levine
 *
 * @created 15/07/2015
 */
public class ExecutionIdByVariableValueIndex extends AbstractVariableValueIndex{
  @Override
  protected String getIndexName() {
    return IndexNames.EXECUTION_ID_BY_VARIABLE_VALUE;
  }

  @Override
  protected String getValue(VariableInstanceEntity entity) {
    return entity.getExecutionId();
  }

}
