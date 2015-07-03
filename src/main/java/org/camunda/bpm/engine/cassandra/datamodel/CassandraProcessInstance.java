package org.camunda.bpm.engine.cassandra.datamodel;

import java.util.ArrayList;
import java.util.List;

import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

public class CassandraProcessInstance {
  
  protected String id;

  protected List<ExecutionEntity> executions = new ArrayList<ExecutionEntity>();

  public List<ExecutionEntity> getExecutions() {
    return executions;
  }

  public void setExecutions(List<ExecutionEntity> executions) {
    this.executions = executions;
  }
}
