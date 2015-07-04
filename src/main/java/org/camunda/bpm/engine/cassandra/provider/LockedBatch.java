package org.camunda.bpm.engine.cassandra.provider;

import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;

public abstract class LockedBatch<T extends DbEntity> {
  
  protected T entity;
  
  protected BatchStatement batch = new BatchStatement();

  protected boolean shouldNotLock = false;
  
  public LockedBatch(T entity) {
    this.entity = entity;
  }
  
  public BatchStatement getBatch() {
    if(!shouldNotLock) {
      addLockStatement(batch);
    }
    return batch;
  }
  
  public boolean isEmpty() {
    return batch.getStatements().isEmpty();
  }
  
  protected abstract void addLockStatement(BatchStatement batch);

  public void addStatement(Statement statement) {
    batch.add(statement);
  }
  
  public void setShouldNotLock() {
    this.shouldNotLock = true;
  }
  
}
