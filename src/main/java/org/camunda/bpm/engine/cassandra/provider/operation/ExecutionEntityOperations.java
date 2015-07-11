package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class ExecutionEntityOperations implements ProcessSubentityOperationsHandler<ExecutionEntity>{
  public final static String ID_IDX_NAME = "exec_id";
    
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
    session.addStatement(createExecutionIdIndex(session, entity));    
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
    session.addIndexStatement(QueryBuilder.delete().all()
        .from(ProcessInstanceTableHandler.INDEX_TABLE_NAME)
        .where(eq("idx_name", ID_IDX_NAME)).and(eq("idx_value",entity.getId())), entity.getProcessInstanceId());
  }

  public void update(CassandraPersistenceSession session, ExecutionEntity entity) {
    session.addStatement(createUpdateStatement(session, entity), entity.getProcessInstanceId());
    session.addIndexStatement(createExecutionIdIndex(session, entity), entity.getProcessInstanceId());
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

  protected Statement createExecutionIdIndex(CassandraPersistenceSession session, ExecutionEntity entity) {
    return QueryBuilder.insertInto(ProcessInstanceTableHandler.INDEX_TABLE_NAME)
        .value("idx_name", ID_IDX_NAME)
        .value("idx_value",entity.getId())
        .value("id",entity.getProcessInstanceId());
  }

  /* (non-Javadoc)
   * @see org.camunda.bpm.engine.cassandra.provider.operation.ProcessSubentityOperationsHandler#getById(java.lang.String)
   */
  @Override
  public ExecutionEntity getById(CassandraPersistenceSession session, String id) {
    Session s = session.getSession();
    Row row = s.execute(select("id").from(ProcessInstanceTableHandler.INDEX_TABLE_NAME).where(eq("idx_name", ID_IDX_NAME)).and(eq("idx_value", id))).one();
    if(row == null) {
      return null;
    }
    String procId = row.getString("id");
    LoadedCompositeEntity loadedCompostite = session.selectCompositeById(ProcessInstanceLoader.NAME, procId);
    if(loadedCompostite==null){
      return null;
    }
    return (ExecutionEntity) loadedCompostite.get(ProcessInstanceLoader.EXECUTIONS).get(id);
  }
}
