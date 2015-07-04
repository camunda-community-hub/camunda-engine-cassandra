package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class VariableEntityOperations implements EntityOperationHandler<VariableInstanceEntity>{

  public void insert(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    session.addStatement(createUpdateStatement(session, entity));
  }

  public void delete(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    session.addStatement(QueryBuilder.delete().mapElt("variables", entity.getId())
      .from(ProcessInstanceTableHandler.TABLE_NAME).where(eq("id", entity.getProcessInstanceId())), entity.getProcessInstanceId());
  }

  public void update(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    session.addStatement(createUpdateStatement(session, entity), entity.getProcessInstanceId());
  }

  protected Statement createUpdateStatement(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    Session s = session.getSession();

    UDTypeHandler typeHandler = session.getTypeHander(VariableInstanceEntity.class);
    CassandraSerializer<VariableInstanceEntity> serializer = session.getSerializer(VariableInstanceEntity.class);

    UDTValue value = typeHandler.createValue(s);
    serializer.write(value, entity);

    return QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
      .with(put("variables", entity.getId(), value))
      .where(eq("id", entity.getProcessInstanceId()));
  }


}
