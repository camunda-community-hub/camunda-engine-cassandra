package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.util.ArrayList;
import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.table.ResourceTableHandler;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class BulkDeleteResourcesByDeploymentId implements BulkOperationHandler {

  public void perform(CassandraPersistenceSession session, Object parameter, BatchStatement flush) {
    
    String deploymentId = (String) parameter;

    Session s = session.getSession();
    
    List<Row> resourcesToDelete = s.execute(QueryBuilder.select("id").from(ResourceTableHandler.TABLE_NAME).where(eq("deployment_id",deploymentId))).all();
    List<String> ids = new ArrayList<String>();
    for (Row row : resourcesToDelete) {
      ids.add(row.getString("id"));
    }
    
    flush.add(delete().all().from(ResourceTableHandler.TABLE_NAME).where(in("id", ids)));
    
  }

}
