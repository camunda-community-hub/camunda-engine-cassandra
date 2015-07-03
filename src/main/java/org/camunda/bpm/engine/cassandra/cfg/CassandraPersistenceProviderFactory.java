package org.camunda.bpm.engine.cassandra.cfg;

import org.camunda.bpm.engine.cassandra.datamodel.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.PersistenceSession;
import org.camunda.bpm.engine.impl.interceptor.Session;
import org.camunda.bpm.engine.impl.interceptor.SessionFactory;

public class CassandraPersistenceProviderFactory implements SessionFactory {

  protected com.datastax.driver.core.Session cassandraSession;

  public CassandraPersistenceProviderFactory(com.datastax.driver.core.Session session) {
    this.cassandraSession = session;
  }

  public Class<?> getSessionType() {
    return PersistenceSession.class;
  }

  public Session openSession() {
    return new CassandraPersistenceSession(cassandraSession);
  }

}
