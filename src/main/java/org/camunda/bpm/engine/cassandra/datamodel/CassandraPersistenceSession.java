package org.camunda.bpm.engine.cassandra.datamodel;

import java.util.List;

import org.camunda.bpm.engine.impl.db.AbstractPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.entitymanager.DbEntityManager;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbBulkOperation;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbEntityOperation;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.Session;

public class CassandraPersistenceSession extends AbstractPersistenceSession {

  protected Session cassandraSession;

  public CassandraPersistenceSession(com.datastax.driver.core.Session session) {
    this.cassandraSession = session;
  }

  public List<?> selectList(String statement, Object parameter) {
    
    if("selectVariablesByExecution".equals(statement)) {
      
    }
    
    return null;
  }

  public <T extends DbEntity> T selectById(Class<T> type, String id) {
    if(type == ExecutionEntity.class) {
      // first check cache
      // first check process instance table
      // if not found, check execution index => load process instance
      // else return null
    }
    return null;
  }

  public Object selectOne(String statement, Object parameter) {
    return null;
  }

  public void lock(String statement, Object parameter) {
    
  }

  public void commit() {
    
  }

  public void rollback() {
    
  }

  public void dbSchemaCheckVersion() {
    
  }

  public void flush() {
    
  }

  public void close() {
    
  }

  protected void insertEntity(DbEntityOperation operation) {
    
  }

  protected void deleteEntity(DbEntityOperation operation) {
    
  }

  protected void deleteBulk(DbBulkOperation operation) {
    
  }

  protected void updateEntity(DbEntityOperation operation) {
    
  }

  protected void updateBulk(DbBulkOperation operation) {
    
  }
  
  
  /// Schema mngt ///////////////////////////////////7

  @Override
  protected String getDbVersion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void dbSchemaCreateIdentity() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaCreateHistory() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaCreateEngine() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaCreateCmmn() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaCreateCmmnHistory() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaDropIdentity() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaDropHistory() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaDropEngine() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaDropCmmn() {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dbSchemaDropCmmnHistory() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean isEngineTablePresent() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isHistoryTablePresent() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isIdentityTablePresent() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isCmmnTablePresent() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isCmmnHistoryTablePresent() {
    // TODO Auto-generated method stub
    return false;
  }


}
