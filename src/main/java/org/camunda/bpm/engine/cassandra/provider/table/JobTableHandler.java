package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.Session;

public class JobTableHandler implements TableHandler {
  
  public final static String TABLE_NAME = "cam_job";
  public final static String JOB_INDEX_TABLE = "cam_job_idx";
  
  protected final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE_NAME +" ("
	      + "id text, "
	      + "type text, "
        + "due_date timestamp, "
        + "lock_exp_time timestamp, "
	      + "lock_owner text, "
	      + "exclusive boolean, "
	      + "execution_id text, "
	      + "process_instance_id text, "
	      + "process_def_id text, "
	      + "process_def_key text, "
	      + "retries int, "
	      + "exception_stack_id text, "
	      + "exception_message text, "
	      + "repeat text, "  				//timer only
	      + "handler_type text, "
	      + "handler_cfg text, "
	      + "deployment_id text, "
	      + "suspension_state int, "
	      + "job_def_id text, "
	      + "sequence_counter bigint, "
        + "priority int, "
	      + "revision int, "
      + "PRIMARY KEY (id));";
  
  protected final static String CREATE_JOB_IDX = "CREATE TABLE IF NOT EXISTS "+JOB_INDEX_TABLE +" ("
      + "shard_id timestamp, "
      + "is_locked boolean, "
      + "priority int, "
      + "sort_time timestamp, "
      + "id text, "
    + "PRIMARY KEY ((shard_id, is_locked, priority), sort_time, id) ) "
    + "WITH CLUSTERING ORDER BY (sort_time ASC);";

  protected final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME;
  protected final static String DROP_INDEX = "DROP TABLE IF EXISTS "+JOB_INDEX_TABLE;
  
  public List<String> getTableNames() {
    return Arrays.asList(TABLE_NAME, JOB_INDEX_TABLE);
  }

  public void createTable(Session s) {
    s.execute(CREATE_TABLE);
    s.execute(CREATE_JOB_IDX);
  }
  
  public void dropTable(Session s) {
    s.execute(DROP_TABLE);
    s.execute(DROP_INDEX);
  }
}
