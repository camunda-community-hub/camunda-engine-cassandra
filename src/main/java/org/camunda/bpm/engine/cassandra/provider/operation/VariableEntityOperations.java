package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.cassandra.provider.type.UDTypeHandler;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class VariableEntityOperations implements EntityOperationHandler<VariableInstanceEntity>{

  public void insert(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    
    Session s = session.getSession();
    
    UDTypeHandler typeHander = session.getTypeHander(VariableInstanceEntity.class);
    CassandraSerializer<VariableInstanceEntity> serializer = session.getSerializer(VariableInstanceEntity.class);
    
    UDTValue value = typeHander.createValue(s);
    serializer.write(value, entity);
    
    session.addStatement(QueryBuilder.update(ProcessInstanceTableHandler.TABLE_NAME)
        .with(put("variables", entity.getId(), value))
        .where(eq("id", entity.getProcessInstanceId())));
  }

  public void delete(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    
  }

  public void update(CassandraPersistenceSession session, VariableInstanceEntity entity) {
    
  }

}
