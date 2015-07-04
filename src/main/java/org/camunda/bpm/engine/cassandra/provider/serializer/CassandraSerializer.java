package org.camunda.bpm.engine.cassandra.provider.serializer;

import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

public interface CassandraSerializer<T extends DbEntity> {
  
  void write(SettableData<?> data, T entity);
  
  T read(GettableData data);

}
