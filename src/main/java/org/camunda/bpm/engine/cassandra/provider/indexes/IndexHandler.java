package org.camunda.bpm.engine.cassandra.provider.indexes;

import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.Statement;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */

public interface IndexHandler <T extends DbEntity>{
  public String getUniqueValue(CassandraPersistenceSession cassandraPersistenceSession, String ... indexValue);
  public List<String> getValues(CassandraPersistenceSession cassandraPersistenceSession, String ... indexValue);
  public Statement getInsertStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity);
  public Statement getDeleteStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity);
  public boolean checkIndexMatch(T entity, String ... indexValues);
  public boolean isUnique();
}
