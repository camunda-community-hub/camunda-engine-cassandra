package org.camunda.bpm.engine.cassandra.cfg;

import org.camunda.bpm.engine.impl.cfg.StandaloneProcessEngineConfiguration;
import org.camunda.bpm.engine.impl.persistence.StrongUuidGenerator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

public class CassandraProcessEngineConfiguration extends StandaloneProcessEngineConfiguration {

  public static final String DEFAULT_KEYSPACE = "camunda";
  protected String cassandraContactPoint;
  protected Cluster cluster;
  protected Session session;
  protected String keyspace;

  @Override
  protected void init() {
    initCassandraClient();
    super.init();
  }
  
  @Override
  protected void initPersistenceProviders() {
    addSessionFactory(new CassandraPersistenceProviderFactory(session));
  }
  
  protected void initCassandraClient() {
    if(keyspace == null) {
      keyspace = DEFAULT_KEYSPACE;
    }

    if(cluster == null) {
      cluster = Cluster.builder()
        .addContactPoint(cassandraContactPoint)
        .build();
      
      // make sure the keyspace exists (create it with default replication settings otherwise)
      KeyspaceMetadata existingKeyspace = cluster.getMetadata().getKeyspace("camunda");
      if(existingKeyspace == null) {
        Session session = cluster.connect();
        session.execute(String.format("CREATE keyspace %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };", keyspace));
        session.close();
      }
      
      session = cluster.connect(keyspace);
    }
  }
  
  protected void initIdGenerator() {
    if(idGenerator == null) {
      idGenerator = new StrongUuidGenerator();
    }
  }
  
  protected void initSqlSessionFactory() {
  }
  
  protected void initDataSource() {
  }
  
  protected void initJpa() {
  }
  
  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }
  
  public Cluster getCluster() {
    return cluster;
  }

  public String getCassandraContactPoint() {
    return cassandraContactPoint;
  }

  public CassandraProcessEngineConfiguration setCassandraContactPoint(String cassandraContactPoint) {
    this.cassandraContactPoint = cassandraContactPoint;
    return this;
  }

  public Session getSession() {
    return session;
  }

  public void setSession(Session session) {
    this.session = session;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public CassandraProcessEngineConfiguration setKeyspace(String keyspace) {
    this.keyspace = keyspace;
    return this;
  }
  
}
