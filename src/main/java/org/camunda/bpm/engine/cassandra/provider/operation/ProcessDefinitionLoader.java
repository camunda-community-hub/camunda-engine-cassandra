package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

public class ProcessDefinitionLoader extends AbstractSingleEntityLoader<ProcessDefinitionEntity> {

  protected Class<ProcessDefinitionEntity> getEntityType() {
    return ProcessDefinitionEntity.class;
  }

  protected String getTableName() {
    return ProcessDefinitionTableHandler.TABLE_NAME;
  }


}
