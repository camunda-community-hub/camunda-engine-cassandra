package org.camunda.bpm.engine.cassandra.provider.type;



public class EventSubscriptionTypeHandler extends AbstractTypeHandler {

  public final static String TYPE_NAME = "event_subscription";
  
  public final static String CREATE = "CREATE TYPE IF NOT EXISTS "+TYPE_NAME+" ("
      + "id text, "
      + "event_type text, "
      + "event_name text, "
      + "execution_id text, "
      + "proc_inst_id text, "
      + "activity_id text, "
      + "configuration text, "
      + "created bigint, "
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
