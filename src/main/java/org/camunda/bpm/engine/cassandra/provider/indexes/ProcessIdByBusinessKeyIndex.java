package org.camunda.bpm.engine.cassandra.provider.indexes;

import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

/**
 * This index is only created for process instances, it is not created for any other executions
 * 
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public class ProcessIdByBusinessKeyIndex extends AbstractIndexHandler<ExecutionEntity> {

  @Override
  protected String getTableName() {
    return ProcessInstanceTableHandler.INDEX_TABLE_NAME;
  }

  @Override
  protected String getIndexName() {
    return IndexNames.PROCESS_ID_BY_BUSINESS_KEY;
  }

  @Override
  protected boolean isUnique() {
    return true;
  }

  @Override
  protected String getIndexValue(ExecutionEntity entity) {
    if(entity.isProcessInstanceExecution()){
      return entity.getBusinessKey();
    }
    return null;
  }

  @Override
  protected String getValue(ExecutionEntity entity) {
    if(entity.isProcessInstanceExecution()){
      return entity.getProcessInstanceId();
    }
    return null;
  }

}
