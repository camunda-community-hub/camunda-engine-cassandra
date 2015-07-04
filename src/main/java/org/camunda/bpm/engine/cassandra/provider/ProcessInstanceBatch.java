package org.camunda.bpm.engine.cassandra.provider;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;

public class ProcessInstanceBatch {
  
  protected ExecutionEntity processInstance;
  
  protected BatchStatement processInstanceBatch = new BatchStatement();

  protected boolean isProcessInstanceDeleted = false;
  
  public ProcessInstanceBatch(ExecutionEntity entity) {
    this.processInstance = entity;
  }
  
  public BatchStatement getBatch() {
    if(!isProcessInstanceDeleted) {
      processInstanceBatch.add(update(ProcessInstanceTableHandler.TABLE_NAME)
          .with(set("version", processInstance.getRevisionNext()))
          .where(eq("id", processInstance.getId()))
          .onlyIf(eq("version", processInstance.getRevision())));
    }
    return processInstanceBatch;
  }
  
  public void addStatement(Statement statement) {
    processInstanceBatch.add(statement);
  }
  
  public void setIsDeleted() {
    this.isProcessInstanceDeleted = true;
  }

}
