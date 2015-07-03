package org.camunda.bpm.engine.cassandra.provider;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class ResourceTableHandler implements TableHandler<ResourceEntity> {
  
  protected final static String TABLE_NAME = "CAM_RESOURCE";

  protected final static String CREATE_TABLE_STMNT = "CREATE TABLE "+TABLE_NAME +" "
      + "(id text, "
      + "name text, "
      + "deployment_id text,"
      + "content blob, "
      + "PRIMARY KEY (id));";
  
  protected final static String INSERT_STMNT = "INSERT into "+TABLE_NAME+" (id, name, deployment_id, content) "
      + "values "
      + "(?, ?, ?, ?);";
  
  
  public void createTable(Session s) {
    s.execute(CREATE_TABLE_STMNT);
  }

  public List<? extends Statement> createInsertStatement(Session s, ResourceEntity data) {
    
    return Collections.singletonList(s.prepare(INSERT_STMNT).bind(
        data.getId(),
        data.getName(),
        data.getDeploymentId(),
        ByteBuffer.wrap(data.getBytes())));
  }

}
