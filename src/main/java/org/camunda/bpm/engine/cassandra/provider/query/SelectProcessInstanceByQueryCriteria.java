package org.camunda.bpm.engine.cassandra.provider.query;

import java.util.Arrays;
import java.util.List;

import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.operation.LoadedCompositeEntity;
import org.camunda.bpm.engine.cassandra.provider.operation.ProcessInstanceLoader;
import org.camunda.bpm.engine.impl.ProcessInstanceQueryImpl;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;


public class SelectProcessInstanceByQueryCriteria implements SelectListQueryHandler<ExecutionEntity, ProcessInstanceQueryImpl> {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public List<ExecutionEntity> executeQuery(CassandraPersistenceSession session, ProcessInstanceQueryImpl parameter) {
    
    if(parameter.getProcessInstanceId() == null) {
      throw new ProcessEngineException("Unsupported process instance query must provide process instance id");
    }
    
    LoadedCompositeEntity composite = session.selectCompositeById(ProcessInstanceLoader.NAME, parameter.getProcessInstanceId());
        
    return (List) Arrays.asList(composite.getMainEntity());
  }

}
