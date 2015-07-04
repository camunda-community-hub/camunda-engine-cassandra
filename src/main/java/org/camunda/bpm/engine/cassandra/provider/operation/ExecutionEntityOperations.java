package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class ExecutionEntityOperations implements EntityOperations<ExecutionEntity>{

  protected final static String INSERT = "INSERT into "+ProcessInstanceTableHandler.TABLE_NAME+" (id, version, business_key) "
      + "values "
      + "(?, ?, ?);";
  
  public void insert(CassandraPersistenceSession session, ExecutionEntity entity, BatchStatement flush) {
    
    Session s = session.getSession();
    
    if(entity.isProcessInstanceExecution()) {
      flush.add(s.prepare(INSERT).bind(
          entity.getId(),
          entity.getRevision(),
          entity.getBusinessKey()));
    }
    
    writeValue(session, entity, flush);
    
  }

  public void delete(CassandraPersistenceSession session, ExecutionEntity entity, BatchStatement flush) {
    
    if(entity.isProcessInstanceExecution()) {
     flush.add(QueryBuilder.delete().all()
          .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId()))
          .onlyIf(eq("version", entity.getRevision())));
     session.resetUpdateLock(entity.getProcessInstanceId());
    }
    else {
      flush.add(QueryBuilder.delete().mapElt("executions", entity.getId())
          .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId())));
      session.ensureUpdateLock(entity.getProcessInstanceId());
    }
  }

  public void update(CassandraPersistenceSession session, ExecutionEntity entity, BatchStatement flush) {
    writeValue(session, entity, flush);
    session.ensureUpdateLock(entity.getProcessInstanceId());
  }

  protected void writeValue(CassandraPersistenceSession session, ExecutionEntity entity, BatchStatement flush) {
    Session s = session.getSession();

    UDTypeHandler typeHander = session.getTypeHander(ExecutionEntity.class);
    CassandraSerializer<ExecutionEntity> serializer = session.getSerializer(ExecutionEntity.class);
    
    UDTValue value = typeHander.createValue(s);
    serializer.write(value, entity);
    
    flush.add(QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(put("executions", entity.getId(), value))
        .where(eq("id", entity.getProcessInstanceId())));
  }

}
