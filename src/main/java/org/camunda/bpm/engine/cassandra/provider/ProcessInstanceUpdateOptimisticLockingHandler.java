package org.camunda.bpm.engine.cassandra.provider;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;

public class ProcessInstanceUpdateOptimisticLockingHandler {
  
  protected ExecutionEntity processInstance;
  
  protected boolean shouldLock = false;
  
  public ProcessInstanceUpdateOptimisticLockingHandler(ExecutionEntity entity) {
    this.processInstance = entity;
  }
  
  public void lock() {
    shouldLock = true;
  }

  public void reset() {
    shouldLock = false;
  }
  
  public void addStatementIfLocked(Session s, BatchStatement flush) {
    if(!shouldLock) {
      return;
    }
    
    flush.add(update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(set("version", processInstance.getRevisionNext()))
        .where(eq("id", processInstance.getId()))
        .onlyIf(eq("version", processInstance.getRevision())));
  }

}
