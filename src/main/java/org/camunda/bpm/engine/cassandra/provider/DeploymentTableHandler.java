package org.camunda.bpm.engine.cassandra.provider;

import java.util.Collections;
import java.util.List;

import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class DeploymentTableHandler implements TableHandler<DeploymentEntity> {
  
  protected final static String TABLE_NAME = "CAM_DEPLOYMENT";

  protected final static String CREATE_TABLE_STMNT = "CREATE TABLE "+TABLE_NAME +" "
      + "(id text, "
      + "name text, "
      + "deploy_time bigint, "
      + "PRIMARY KEY (id));";
  
  protected final static String INSERT_STMNT = "INSERT into "+TABLE_NAME+" (id, name, deploy_time) "
      + "values "
      + "(?, ?, ?);";
  
  public void createTable(Session s) {
    s.execute(CREATE_TABLE_STMNT);
  }

  public List<? extends Statement> createInsertStatement(Session s, DeploymentEntity data) {
    return Collections.singletonList(s.prepare(INSERT_STMNT).bind(
        data.getId(),
        data.getName(),
        data.getDeploymentTime().getTime()));     
  }  

}
