package org.camunda.bpm.engine.cassandra.provider.query;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SelectLatestProcessDefinitionByKeyQueryHandler implements SingleResultQueryHandler<ProcessDefinitionEntity> {
  
  public ProcessDefinitionEntity executeQuery(CassandraPersistenceSession session, Object parameter) {
    Session s = session.getSession();
    
    Row row = s.execute(select("id")
      .from(ProcessDefinitionTableHandler.TABLE_NAME_IDX_VERSION)
      .where(eq("key", parameter))
      .limit(1))
      .one();
    
    if(row == null) {
      return null;
    }
    
    String id = row.getString("id");

    Row result = s.execute(select()
        .all()
        .from(ProcessDefinitionTableHandler.TABLE_NAME)
        .where(eq("id", id)))
        .one();
    
    if(result == null) {
      return null;
    }
    
    CassandraSerializer<ProcessDefinitionEntity> serializer = session.getSerializer(ProcessDefinitionEntity.class);
    
    return serializer.read(result);
    
  }

}
