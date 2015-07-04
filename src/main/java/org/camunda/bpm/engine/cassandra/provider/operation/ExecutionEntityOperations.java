package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class ExecutionEntityOperations implements EntityOperationHandler<ExecutionEntity>{

  protected final static String INSERT = "INSERT into "+ProcessInstanceTableHandler.TABLE_NAME+" (id, version, business_key) "
      + "values "
      + "(?, ?, ?);";
  
  public void insert(CassandraPersistenceSession session, ExecutionEntity entity) {
    
    Session s = session.getSession();
    
    if(entity.isProcessInstanceExecution()) {
      session.addStatement(s.prepare(INSERT).bind(
          entity.getId(),
          entity.getRevision(),
          entity.getBusinessKey()));
    }
    
    session.addStatement(createUpdateStatement(session, entity));
    
  }

  public void delete(CassandraPersistenceSession session, ExecutionEntity entity) {
    
    if(entity.isProcessInstanceExecution()) {
     session.addStatement(QueryBuilder.delete().all()
          .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId()))
          .onlyIf(eq("version", entity.getRevision())),
          entity.getProcessInstanceId());
     session.batchShouldNotLock(entity.getProcessInstanceId());
    }
    else {
      session.addStatement(QueryBuilder.delete().mapElt("executions", entity.getId())
          .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId())),
          entity.getProcessInstanceId());
    }
  }

  public void update(CassandraPersistenceSession session, ExecutionEntity entity) {
    session.addStatement(createUpdateStatement(session, entity), entity.getProcessInstanceId());
  }

  protected Statement createUpdateStatement(CassandraPersistenceSession session, ExecutionEntity entity) {
    Session s = session.getSession();

    UDTypeHandler typeHander = session.getTypeHander(ExecutionEntity.class);
    CassandraSerializer<ExecutionEntity> serializer = session.getSerializer(ExecutionEntity.class);
    
    UDTValue value = typeHander.createValue(s);
    serializer.write(value, entity);
    
    return QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(put("executions", entity.getId(), value))
        .where(eq("id", entity.getProcessInstanceId()));
  }

}
