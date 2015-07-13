package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler.TABLE_NAME;
import static org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler.TABLE_NAME_IDX_VERSION;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class ProcessDefinitionOperations extends AbstractEntityOperationHandler<ProcessDefinitionEntity> {
  
  protected final static String INSERT = "INSERT into "+TABLE_NAME+" (id, key, version, category, name, deployment_id, suspension_state) "
      + "values "
      + "(?, ?, ?, ?, ?, ?, ?);";

  protected final static String INSERT_IDX_VERSION = "INSERT into "+TABLE_NAME_IDX_VERSION+" (key, version, id) "
      + "values "
      + "(?, ?, ?);";
  
  public void insert(CassandraPersistenceSession session, ProcessDefinitionEntity entity) {
    Session s = session.getSession();

    CassandraSerializer<ProcessDefinitionEntity> serializer = session.getSerializer(ProcessDefinitionEntity.class);
   
    // insert deployment
    BoundStatement statement = s.prepare(INSERT).bind();    
    serializer.write(statement, entity);     
    session.addStatement(statement);
    
    // write index
    session.addStatement(s.prepare(INSERT_IDX_VERSION).bind(
        entity.getKey(),
        entity.getVersion(),
        entity.getId()));
  }

  public void delete(CassandraPersistenceSession session, ProcessDefinitionEntity entity) {
    
    session.addStatement(QueryBuilder.delete().all().from(TABLE_NAME).where(eq("id", entity.getId())));
    session.addStatement(QueryBuilder.delete().all().from(INSERT_IDX_VERSION).where(eq("key", entity.getKey())).and(eq("version", entity.getVersion())));
    
  }

  public void update(CassandraPersistenceSession session, ProcessDefinitionEntity entity) {
    
  }

  protected Class<ProcessDefinitionEntity> getEntityType() {
    return ProcessDefinitionEntity.class;
  }

  protected String getTableName() {
    return ProcessDefinitionTableHandler.TABLE_NAME;
  }


}
