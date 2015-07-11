package org.camunda.bpm.engine.cassandra.provider.query;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public abstract class AbstractIndexQueryHandler implements IndexQueryHandler {
  protected abstract String getTableName();
  protected abstract String getIndexName();
  
  @Override
  public String getIdByIndex(CassandraPersistenceSession cassandraPersistenceSession, String indexName, String indexValue) {
    Session s = cassandraPersistenceSession.getSession();
    Row row = s.execute(select("id").from(getTableName()).where(eq("idx_name", indexName)).and(eq("idx_value", indexValue))).one();
    if(row == null) {
      return null;
    }
    return row.getString("id");
  }

}
