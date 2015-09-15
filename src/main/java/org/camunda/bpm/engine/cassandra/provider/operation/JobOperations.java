package org.camunda.bpm.engine.cassandra.provider.operation;

import static org.camunda.bpm.engine.cassandra.provider.table.JobTableHandler.JOB_INDEX_TABLE;
import static org.camunda.bpm.engine.cassandra.provider.table.JobTableHandler.TABLE_NAME;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.JobTableHandler;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.EntityLoadListener;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class JobOperations extends AbstractEntityOperationHandler<JobEntity> implements EntityLoadListener{
	//TODO - must have ZooKeper locking for this because we have to lock both job and the process in one transaction.
  //currently it only locks the process
  
	private final static String INSERT = "INSERT into "+TABLE_NAME+" ("
	      + "id, "
	      + "type, "
        + "due_date, "
        + "lock_exp_time, "
	      + "lock_owner, "
	      + "exclusive, "
	      + "execution_id, "
	      + "process_instance_id, "
	      + "process_def_id, "
	      + "process_def_key, "
	      + "retries, "
	      + "exception_stack_id, "
	      + "exception_message, "
	      + "repeat, "  				
	      + "handler_type, "
	      + "handler_cfg, "
	      + "deployment_id, "
	      + "suspension_state, "
	      + "job_def_id, "
	      + "sequence_counter, "
        + "priority, "
	      + "revision"
  		+ ") "
      + "values "
      + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

  private final static String DELETE = "DELETE FROM "+TABLE_NAME+" WHERE "
      + "id = '?';";

  private final static String INSERT_INDEX = "INSERT into "+JOB_INDEX_TABLE+" ("
      + "shard_id, "
      + "is_locked, "
      + "priority, "
      + "sort_time, "
      + "id"
    + ") values "
    + "(?, ?, ?, ?, ?);";

  private final static String DELETE_INDEX = "DELETE FROM "+JOB_INDEX_TABLE+" WHERE "
  		  + "shard_id = ? AND "
        + "is_locked = ? AND "
        + "priority = ? AND "
	      + "sort_time = ? AND "
	      + "id = '?';";

  private final static String SELECT_ACTIVE = "SELECT * FROM "+JOB_INDEX_TABLE+" WHERE "
      + "shard_id = ? AND "
      + "is_locked = ? AND "
      + "priority = ? "
      + "LIMIT ?;";

  private static PreparedStatement insertStatement=null;
  private static PreparedStatement deleteStatement=null;
  private static PreparedStatement insertIndexStatement=null;
  private static PreparedStatement deleteIndexStatement=null;
  private static PreparedStatement selectActiveStatement=null;
  
  protected static int shardSizeMillis; //size of the job shard
  protected static int shardInitNumber; //how far to go back to find active shards on start-up 
  protected static int maxPriority; //maximum possible priority
  
  protected static List<Long> activeShards = Collections.synchronizedList(new LinkedList<Long>());
  protected static List<Long> activeLockedShards = Collections.synchronizedList(new LinkedList<Long>());
  
  private Map<String, JobEntity> entityCache=new HashMap<String,JobEntity>();
    
  public JobOperations(CassandraPersistenceSession cassandraPersistenceSession) {
    cassandraPersistenceSession.addEntityLoadListener(this);
  }

  public static void prepare(CassandraProcessEngineConfiguration config) {
      insertStatement = config.getSession().prepare(INSERT);
      deleteStatement = config.getSession().prepare(DELETE);
      insertIndexStatement = config.getSession().prepare(INSERT_INDEX);
      deleteIndexStatement = config.getSession().prepare(DELETE_INDEX);
      selectActiveStatement = config.getSession().prepare(SELECT_ACTIVE);
      
      shardSizeMillis = config.getJobShardSizeHours()*3600*1000;
      shardInitNumber=config.getJobShardSizeHours();
      maxPriority=config.getMaxPriority();
      
      long currentShard = calculateShard(System.currentTimeMillis()); 
      //NOTE - locked indexes are sorted and sharded by lock expiry date and
      //unlocked indexes are sorted and sharded by the job due date
      for(int i=0;i<shardInitNumber;i++){
        currentShard = calculateShard(currentShard-1);//start with the current shard - 1
        for(int j=0; j<=maxPriority;j++){
          if(checkActive(currentShard, false, j, config.getSession()) && activeShards.get(0)!=currentShard){
            activeShards.add(0, currentShard); 
          }
          if(checkActive(currentShard, true, j, config.getSession()) && activeLockedShards.get(0)!=currentShard){
            activeLockedShards.add(0, currentShard); 
          }
        }
      }
  }
  
  public void insert(CassandraPersistenceSession session, JobEntity entity) {
    checkPriority(entity);
    if(entity.getDuedate()==null){
      entity.setDuedate(new Date()); //we must have due date for sharding
    }
    
    CassandraSerializer<JobEntity> serializer = CassandraPersistenceSession.getSerializer(JobEntity.class);
   
    BoundStatement statement = insertStatement.bind();    
    serializer.write(statement, entity);     
    session.addStatement(statement);
   
    bindIndexStatement(session, entity, insertIndexStatement);
    
    entityCache.put(entity.getId(), entity);
  }
  
  protected void bindIndexStatement(CassandraPersistenceSession session, JobEntity entity, PreparedStatement statement) {
    boolean isLocked = isLocked(entity);
    BoundStatement indexStatement = statement.bind();
    long shard=calculateShard(isLocked ? entity.getLockExpirationTime().getTime() : entity.getDuedate().getTime());
    indexStatement.setDate("shard_id", new Date(shard));
    indexStatement.setBool("is_locked", isLocked);
    indexStatement.setInt("priority", entity.getPriority());
    indexStatement.setDate("sort_time", isLocked ? entity.getLockExpirationTime() : entity.getDuedate());
    indexStatement.setString("id", entity.getId());
    session.addStatement(indexStatement);    
  }

  public void delete(CassandraPersistenceSession session, JobEntity entity) {
    session.addStatement(deleteStatement.bind(entity.getId()));
    JobEntity oldEntity = getCachedEntity(entity);
    bindIndexStatement(session, oldEntity, deleteIndexStatement);
  }

  public void update(CassandraPersistenceSession session, JobEntity entity) {
    checkPriority(entity);
    JobEntity oldEntity = getCachedEntity(entity);
    if(!oldEntity.getDuedate().equals(entity.getDuedate()) ||
        lockExpiryChanged(entity, oldEntity) ||
        oldEntity.getPriority()!=entity.getPriority()){
      //changed the key fields - have to delete / insert the index
      if(!oldEntity.getDuedate().equals(entity.getDuedate()) || (isLocked(oldEntity) && !isLocked(entity))){
        //changed the due date or unlocked - need to check if the due date is in the past and reset it to now if so.
        //it's either this or throw an exception as we are relying on the fact that the old inactive shards stay inactive
         if(entity.getDuedate()==null||entity.getDuedate().getTime() < System.currentTimeMillis()){
          entity.setDuedate(new Date()); 
        }
      }
      bindIndexStatement(session, oldEntity, deleteIndexStatement);
    }
    insert(session, entity);
  }

  @Override
  public void onEntityLoaded(DbEntity entity) {
    if(entity instanceof JobEntity){
      CassandraSerializer<JobEntity> serializer = CassandraPersistenceSession.getSerializer(JobEntity.class);
      JobEntity copy= serializer.copy((JobEntity) entity);
      entityCache.put(entity.getId(), copy);
    }
  }
  
  protected Class<JobEntity> getEntityType() {
    return JobEntity.class;
  }

  protected String getTableName() {
    return JobTableHandler.TABLE_NAME;
  }

  public static long calculateShard(long date){
     double shardSequenceNumber=Math.floor(date/shardSizeMillis);
     long shardId=(long) (shardSequenceNumber*shardSizeMillis);
     return shardId;
  }

  private static boolean checkActive(long shard, boolean locked, int priority, Session session){
    BoundStatement statement = selectActiveStatement.bind();
    statement.setDate("shard_id", new Date(shard));
    statement.setBool("is_locked", locked);
    statement.setInt("priority", priority);
    statement.setInt(3, 1); //limit, just get one
    Row row = session.execute(statement).one();
    return row!=null;
  }

  private void checkPriority(JobEntity entity){
    if(entity.getPriority()<0 || entity.getPriority()>maxPriority){
      throw new IllegalArgumentException("Job priority out of range: "+entity.getPriority()+". Configured maximum priority is "+maxPriority);
    }    
  }
  
  private boolean isLocked(JobEntity entity){
    return entity.getLockExpirationTime()!=null;
  }
  
  private boolean lockExpiryChanged(JobEntity entity, JobEntity oldEntity){
    if(oldEntity.getLockExpirationTime()==null) {
      return entity.getLockExpirationTime() != null;
    }
    return !oldEntity.getLockExpirationTime().equals(entity.getLockExpirationTime());
  }
  
  private JobEntity getCachedEntity(JobEntity entity){
    JobEntity oldEntity = entityCache.get(entity.getId());
    if(oldEntity==null){
      throw new RuntimeException("Inconsistent state, entity needs to be loaded into command context before it can be updated.");
    }
    return oldEntity;
  }

  public static List<Long> getActiveShards() {
    return activeShards;
  }

  public static List<Long> getActiveLockedShards() {
    return activeLockedShards;
  }
}
