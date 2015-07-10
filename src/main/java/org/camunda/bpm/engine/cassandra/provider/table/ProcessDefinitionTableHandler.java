package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.Session;

public class ProcessDefinitionTableHandler implements TableHandler {
  
  public final static String TABLE_NAME = "CAM_PROC_DEF";
  public final static String TABLE_NAME_IDX_VERSION = "CAM_PROC_DEF_IDX_VERSION";

  protected final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE_NAME +" "
      + "(id text, "
      + "key text, "
      + "version int, "
      + "category text, "
      + "name text, "
      + "deployment_id text, "
      + "suspension_state int, "
      + "PRIMARY KEY (id));";
  
  protected final static String CREATE_TABLE_IDX_VERSION = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME_IDX_VERSION +" "
      + "(key text, "
      + "version int, "
      + "id text, "
      + "PRIMARY KEY (key,version)) WITH CLUSTERING ORDER BY (version DESC);";
  
  protected final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME;
  
  protected final static String DROP_TABLE_IDX_VERSION = "DROP TABLE IF EXISTS "+TABLE_NAME_IDX_VERSION;

  protected final static String DEPLOYMENT_ID_IDX = "CREATE INDEX IF NOT EXISTS ON "  + TABLE_NAME + " ( deployment_id );";


  public List<String> getTableNames() {
    return Arrays.asList(TABLE_NAME, TABLE_NAME_IDX_VERSION);
  }

  public void createTable(Session s) {
    s.execute(CREATE_TABLE);
    s.execute(CREATE_TABLE_IDX_VERSION);
    s.execute(DEPLOYMENT_ID_IDX);
  }
  
  public void dropTable(Session s) {
    s.execute(DROP_TABLE_IDX_VERSION);
    s.execute(DROP_TABLE);
  }
//  
//  public List<? extends Statement> createInsertStatement(Session s, ProcessDefinitionEntity procDef) {
//    
//    return Arrays.asList(
//        s.prepare(INSERT).bind(
//          procDef.getId(),
//          procDef.getKey(),
//          procDef.getVersion(),
//          procDef.getCategory(),
//          procDef.getName(),
//          procDef.getDeploymentId(),
//          procDef.getSuspensionState()),
//          
//        s.prepare(INSERT_IDX_VERSION).bind(
//            procDef.getKey(),
//            procDef.getVersion(),
//            procDef.getId()));
//  }
//  
////
//  public ProcessDefinitionEntity selectLatestProcessDefinitionByKey(Session s, String processDefinitionKey) {
//    Row row = s.execute(select("id")
//      .from(TABLE_NAME_IDX_VERSION)
//      .where(eq("key", processDefinitionKey))
//      .limit(1))
//      .one();
//    if(row == null) {
//      return null;
//    }
//    String id = row.getString("id");
//    return selectById(s, id);
//  }
//
//  public ProcessDefinitionEntity selectById(Session s, String id) {
//    Row row = s.execute(select().all().from(TABLE_NAME).where(eq("id", id))).one();
//    if(row == null) {
//      return null;
//    }
//    
//    ProcessDefinitionEntity entity = new ProcessDefinitionEntity();
//    entity.setId(id);
//    entity.setKey(row.getString("key"));
//    entity.setVersion(row.getInt("version"));
//    entity.setCategory(row.getString("category"));
//    entity.setName(row.getString("name"));
//    entity.setDeploymentId(row.getString("deployment_id"));
//    entity.setSuspensionState(row.getInt("suspension_state"));
//    return entity;
//  }
//  

}
