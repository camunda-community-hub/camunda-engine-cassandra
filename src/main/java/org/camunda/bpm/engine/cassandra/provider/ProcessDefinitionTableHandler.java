package org.camunda.bpm.engine.cassandra.provider;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.Arrays;
import java.util.List;

import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class ProcessDefinitionTableHandler implements TableHandler<ProcessDefinitionEntity> {
  
  protected final static String TABLE_NAME = "CAM_PROC_DEF";
  protected final static String TABLE_NAME_IDX_VERSION = "CAM_PROC_DEF_IDX_VERSION";

  protected final static String CREATE_TABLE_STMNT = "CREATE TABLE "+TABLE_NAME +" "
      + "(id text, "
      + "key text, "
      + "version int, "
      + "category text, "
      + "name text, "
      + "deployment_id text, "
      + "suspension_state int, "
      + "PRIMARY KEY (id));";
  
  protected final static String CREATE_VERSION_IDX_TABLE_STMNT = "CREATE TABLE " + TABLE_NAME_IDX_VERSION +" "
      + "(key text, "
      + "version int, "
      + "id text, "
      + "PRIMARY KEY (key,version)) WITH CLUSTERING ORDER BY (version DESC);";
  
  protected final static String INSERT_STMNT = "INSERT into "+TABLE_NAME+" (id, key, version, category, name, deployment_id, suspension_state) "
      + "values "
      + "(?, ?, ?, ?, ?, ?, ?);";

  protected final static String INSERT_STMNT_VERSION_IDX = "INSERT into "+TABLE_NAME_IDX_VERSION+" (key, version, id) "
      + "values "
      + "(?, ?, ?);";
  
  
  public void createTable(Session s) {
    s.execute(CREATE_TABLE_STMNT);
    s.execute(CREATE_VERSION_IDX_TABLE_STMNT);
  }
  
  public List<? extends Statement> createInsertStatement(Session s, ProcessDefinitionEntity procDef) {
    
    return Arrays.asList(
        s.prepare(INSERT_STMNT).bind(
          procDef.getId(),
          procDef.getKey(),
          procDef.getVersion(),
          procDef.getCategory(),
          procDef.getName(),
          procDef.getDeploymentId(),
          procDef.getSuspensionState()),
          
        s.prepare(INSERT_STMNT_VERSION_IDX).bind(
            procDef.getKey(),
            procDef.getVersion(),
            procDef.getId()));
  }
  

  public ProcessDefinitionEntity selectLatestProcessDefinitionByKey(Session s, String processDefinitionKey) {
    Row row = s.execute(select("id")
        .from(TABLE_NAME_IDX_VERSION)
        .where(eq("key", processDefinitionKey))
        .limit(1))
        .one();
    if(row == null) {
      return null;
    }
    String id = row.getString("id");
    return selectById(s, id);
  }

  private ProcessDefinitionEntity selectById(Session s, String id) {
    Row row = s.execute(select().all().from(TABLE_NAME).where(eq("id", id))).one();
    if(row == null) {
      return null;
    }
    
    ProcessDefinitionEntity entity = new ProcessDefinitionEntity();
    entity.setId(id);
    entity.setKey(row.getString("key"));
    entity.setVersion(row.getInt("version"));
    entity.setCategory(row.getString("category"));
    entity.setName(row.getString("name"));
    entity.setDeploymentId(row.getString("deployment_id"));
    entity.setSuspensionState(row.getInt("suspension_state"));
    return entity;
  }
  

}
