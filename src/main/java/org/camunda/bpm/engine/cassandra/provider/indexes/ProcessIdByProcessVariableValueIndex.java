package org.camunda.bpm.engine.cassandra.provider.indexes;

import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

/**
 * @author Natalia Levine
 *
 * @created 15/07/2015
 */
public class ProcessIdByProcessVariableValueIndex extends AbstractVariableValueIndex {

  @Override
  protected String getIndexName() {
    return IndexNames.PROCESS_ID_BY_PROCESS_VARIABLE_VALUE;
  }

  @Override
  protected String getValue(VariableInstanceEntity entity) {
    if(entity.getProcessInstanceId().equals(entity.getExecutionId())){
      return entity.getExecutionId();
    }
    return null;
  }

}
