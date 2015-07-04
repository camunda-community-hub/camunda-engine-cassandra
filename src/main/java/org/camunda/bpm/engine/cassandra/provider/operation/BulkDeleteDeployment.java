package org.camunda.bpm.engine.cassandra.provider.operation;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.table.DeploymentTableHandler;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class BulkDeleteDeployment implements BulkOperationHandler {

  public void perform(CassandraPersistenceSession session, Object parameter, BatchStatement flush) {
    String deploymentId = (String) parameter;
    
    flush.add(QueryBuilder.delete().all().from(DeploymentTableHandler.TABLE_NAME).where(eq("id", deploymentId)));
  }

}
