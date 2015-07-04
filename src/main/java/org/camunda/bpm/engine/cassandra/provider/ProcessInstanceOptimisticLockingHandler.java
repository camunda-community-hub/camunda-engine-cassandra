package org.camunda.bpm.engine.cassandra.provider;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;

public class ProcessInstanceOptimisticLockingHandler {
  
  protected ExecutionEntity processInstance;
  
  protected boolean isStatementCreated = false;
  
  public ProcessInstanceOptimisticLockingHandler(ExecutionEntity entity) {
    this.processInstance = entity;
  }
  
  public void lock(Session s, BatchStatement batch) {
    if(isStatementCreated) {
      return;
    }
    
    batch.add(update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(set("version", processInstance.getRevisionNext()))
        .where(eq("id", processInstance.getId()))
        .onlyIf(eq("version", processInstance.getRevision())));
    
    
  }

}
