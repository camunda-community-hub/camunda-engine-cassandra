package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public abstract class AbstractEntityOperationHandler<T extends DbEntity> implements EntityOperationHandler<T> {

  public T getEntityById(CassandraPersistenceSession cassandraPersistenceSession, String id) {

    Session s = cassandraPersistenceSession.getSession();
    
    Row row = s.execute(select().all().from(getTableName()).where(eq("id", id))).one();
    if(row == null) {
      return null;
    }
    
    CassandraSerializer<T> serializer = cassandraPersistenceSession.getSerializer(getEntityType());
    
    return serializer.read(row);
  }
  
  protected abstract Class<T> getEntityType();
  
  protected abstract String getTableName();

}
