package org.camunda.bpm.engine.cassandra.provider.query;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */

public interface IndexQueryHandler {
  public String getIdByIndex(CassandraPersistenceSession cassandraPersistenceSession, String indexName, String indexValue);
}
