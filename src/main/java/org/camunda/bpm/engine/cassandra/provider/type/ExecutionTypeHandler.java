package org.camunda.bpm.engine.cassandra.provider.type;



public class ExecutionTypeHandler extends AbstractTypeHandler {

  public final static String TYPE_NAME = "execution";
    
  public final static String CREATE = "CREATE TYPE IF NOT EXISTS "+TYPE_NAME+" ("
      + "id text, "
      + "proc_inst_id text, "
      + "parent_id text, "
      + "proc_def_id text, "
      + "super_exec text, "
      + "super_case_exec text, "
      + "case_inst_id text, "
      + "act_inst_id text, "
      + "act_id text, "
      + "is_active boolean, "
      + "is_concurrent boolean, "
      + "is_scope boolean, "
      + "is_event_scope boolean, "
      + "suspension_state int, "
      + "cached_ent_state int, "
      + "sequence_counter bigint, "
      + ");";
  
  public final static String DROP = "DROP TYPE IF EXISTS "+TYPE_NAME+";";
  
  public String getTypeName() {
    return TYPE_NAME;
  }
  
  protected String getCreateStatement() {
    return CREATE;
  }
  
  protected String getDropStatement() {
    return DROP;
  }
  
}
