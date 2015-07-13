package org.camunda.bpm.engine.cassandra.provider.indexes;

import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public class ProcessIdByVariableIdIndex extends AbstractIndexHandler<VariableInstanceEntity> {

  @Override
  protected String getTableName() {
    return ProcessInstanceTableHandler.INDEX_TABLE_NAME;
  }

  @Override
  protected String getIndexName() {
    return IndexNames.PROCESS_ID_BY_VARIABLE_ID;
  }

  @Override
  protected boolean isUnique() {
    return true;
  }

  @Override
  protected String getIndexValue(VariableInstanceEntity entity) {
    return entity.getId();
  }

  @Override
  protected String getValue(VariableInstanceEntity entity) {
    return entity.getProcessInstanceId();
  }

}
