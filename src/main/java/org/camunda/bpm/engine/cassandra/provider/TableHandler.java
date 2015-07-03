package org.camunda.bpm.engine.cassandra.provider;

import java.util.List;

import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public interface TableHandler<T> {
  
  void createTable(Session s);
  
  List<? extends Statement> createInsertStatement(Session s, T entity);

}
