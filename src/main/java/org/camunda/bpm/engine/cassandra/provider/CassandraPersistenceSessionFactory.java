package org.camunda.bpm.engine.cassandra.provider;

import org.camunda.bpm.engine.impl.db.PersistenceSession;
import org.camunda.bpm.engine.impl.interceptor.Session;
import org.camunda.bpm.engine.impl.interceptor.SessionFactory;

public class CassandraPersistenceSessionFactory implements SessionFactory {

  protected com.datastax.driver.core.Session cassandraSession;

  public CassandraPersistenceSessionFactory(com.datastax.driver.core.Session session) {
    this.cassandraSession = session;
  }

  public Class<?> getSessionType() {
    return PersistenceSession.class;
  }

  public Session openSession() {
    return new CassandraPersistenceSession(cassandraSession);
  }

}
