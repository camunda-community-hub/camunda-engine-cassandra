/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static org.camunda.bpm.engine.cassandra.provider.table.JobTableHandler.JOB_INDEX_TABLE;
import static org.camunda.bpm.engine.cassandra.provider.table.JobTableHandler.TABLE_NAME;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExclusiveJobsByDueDateIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExclusiveJobsByLockExpiryIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.JobsByConfigurationIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.JobsByExecutionIdIndex;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.JobEntityKey;
import org.camunda.bpm.engine.cassandra.provider.table.JobTableHandler;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.EntityLoadListener;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Jobs are essentially stored in a queue, which is a bad idea in Cassandra generally. 
 * The queue is sharded to avoid the tombstone issues, however it will still cause hot spots in the cassandra cluster. 
 * This implementation can support small to medium throughput (please don't ask me for specific numbers)
 * For medium / large implementations use a proper queue. 
 * 
 * Unlocked jobs are stored separately from the locked jobs. 
 * Unlocked jobs are sorted by job due date and are always returned in this order.
 * Locked jobs are sorted by the lock expiry time. The jobs where the locks have expired are always selected before 
 * unlocked jobs on the premise that they have already gone through the job prioritization waited for the lock expiry interval 
 * and generally need to be executed yesterday  
 * 
 * After much dithering we've decided not to support priorities, use a queue if priorities are required
 *  
 * @author Natalia Levine
 *
 * @created 15/09/2015
 */

public class JobOperations extends AbstractEntityOperationHandler<JobEntity> implements EntityLoadListener {
  
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
      + "id = ?;";

/*  private final static String SELECT = "SELECT * FROM "+TABLE_NAME+" WHERE "
      + "id = ? "
      + ";";
*/
  
  private final static String INSERT_INDEX = "INSERT into "+JOB_INDEX_TABLE+" ("
      + "shard_id, "
      + "is_locked, "
      + "sort_time, "
      + "id"
    + ") values "
    + "(?, ?, ?, ?);";

  private final static String DELETE_INDEX = "DELETE FROM "+JOB_INDEX_TABLE+" WHERE "
  		  + "shard_id = ? AND "
        + "is_locked = ? AND "
	      + "sort_time = ? AND "
	      + "id = ? ;";

  private static PreparedStatement insertStatement=null;
  private static PreparedStatement deleteStatement=null;
  //private static PreparedStatement selectStatement=null;
  private static PreparedStatement insertIndexStatement=null;
  private static PreparedStatement deleteIndexStatement=null;
  
  protected static int shardSizeMillis; //size of the job shard
  protected static int shardInitNumber; //how far to go back to find active shards on start-up 
  //protected static int maxPriority; //maximum possible priority
  
  protected static Map<Class<?>, IndexHandler<JobEntity>> indexHandlers = new HashMap<Class<?>, IndexHandler<JobEntity>>();

  static {
    indexHandlers.put(JobsByConfigurationIndex.class, new JobsByConfigurationIndex());
    indexHandlers.put(ExclusiveJobsByDueDateIndex.class, new ExclusiveJobsByDueDateIndex());
    indexHandlers.put(ExclusiveJobsByLockExpiryIndex.class, new ExclusiveJobsByLockExpiryIndex());
    indexHandlers.put(JobsByExecutionIdIndex.class, new JobsByExecutionIdIndex());
  }
  
  private Map<String, JobEntity> entityCache=new HashMap<String,JobEntity>();
  
  public JobOperations(CassandraPersistenceSession cassandraPersistenceSession) {
    cassandraPersistenceSession.addEntityLoadListener(this);
  }

  public static void prepare(CassandraProcessEngineConfiguration config) {
      insertStatement = config.getSession().prepare(INSERT);
      deleteStatement = config.getSession().prepare(DELETE);
    //  selectStatement = config.getSession().prepare(SELECT);
      insertIndexStatement = config.getSession().prepare(INSERT_INDEX);
      deleteIndexStatement = config.getSession().prepare(DELETE_INDEX);
     /* deleteIndexStatement = config.getSession().prepare(QueryBuilder.delete().all()
          .from(JOB_INDEX_TABLE)
          .where(eq("shard_id", QueryBuilder.bindMarker()))
          .and(eq("is_locked",QueryBuilder.bindMarker()))
          .and(eq("sort_time",QueryBuilder.bindMarker()))
          .and(eq("id",QueryBuilder.bindMarker())));*/
      

      
      shardSizeMillis = config.getJobShardSizeHours()*3600*1000;
      shardInitNumber=config.getJobShardInitNumber();
      //maxPriority=config.getMaxPriority();
  }
  
  public void insert(CassandraPersistenceSession session, JobEntity entity) {
   // checkPriority(entity);
    if(entity.getDuedate()==null){
      entity.setDuedate(new Date()); //we must have due date for sharding
    }
    
    CassandraSerializer<JobEntity> serializer = CassandraPersistenceSession.getSerializer(JobEntity.class);
   
    BoundStatement statement = insertStatement.bind();    
    serializer.write(statement, entity);     
    session.addStatement(statement);

    insertIndex(session, entity);
    
    for(IndexHandler<JobEntity> index:indexHandlers.values()){
      session.addStatement(index.getInsertStatement(session,entity));    
    }
    
    entityCache.put(entity.getId(), entity);
  }
  
  protected void insertIndex(CassandraPersistenceSession session, JobEntity entity) {
    BoundStatement indStatement = insertIndexStatement.bind();    
    bindKeyFields(session, entity, indStatement);
    session.addStatement(indStatement);
  }

  protected void bindKeyFields(CassandraPersistenceSession session, JobEntity entity, BoundStatement statement) {
    JobEntityKey key = new JobEntityKey(entity, shardSizeMillis);
    statement.setDate("shard_id", new Date(key.getShardId()));
    statement.setBool("is_locked", key.isLocked());
    statement.setDate("sort_time", new Date(key.getSortTime()));
    statement.setString("id", key.getId());
  }

  public void delete(CassandraPersistenceSession session, JobEntity entity) {
    BoundStatement statement = deleteStatement.bind(entity.getId());    
    session.addStatement(statement);

    JobEntity oldEntity = getCachedEntity(entity);
    deleteIndex(session, oldEntity);
    
    for(IndexHandler<JobEntity> index:indexHandlers.values()){
      session.addStatement(index.getDeleteStatement(session,oldEntity));    
    }
  }

  protected void deleteIndex(CassandraPersistenceSession session, JobEntity entity) {
    BoundStatement indStatement = deleteIndexStatement.bind();    
    bindKeyFields(session, entity, indStatement);
    session.addStatement(indStatement);    
  }

  public void update(CassandraPersistenceSession session, JobEntity entity) {
   // checkPriority(entity);
    JobEntity oldEntity = getCachedEntity(entity);
    long now = System.currentTimeMillis();
    if( entity.getDuedate() == null ) {
      entity.setDuedate(new Date(now));  
    }

    JobEntityKey newKey= new JobEntityKey(entity, shardSizeMillis);
    JobEntityKey oldKey= new JobEntityKey(oldEntity, shardSizeMillis);
    if(!newKey.equals(oldKey) ){
      //changed the key fields - have to delete / insert 

      //if the sort time has changed to some time in the past we need to reset it to now.
      //it's either this or throw an exception as we are relying on the fact that the old inactive shards stay inactive
      //this should not happen anyway 
      if(newKey.isLocked()) {
        if( !entity.getLockExpirationTime().equals(oldEntity.getLockExpirationTime()) &&
            entity.getLockExpirationTime().getTime() < now){
          entity.setLockExpirationTime(new Date(now));
        }
      }
      else if(!oldEntity.getDuedate().equals(entity.getDuedate()) &&
          entity.getDuedate().getTime() < now){
        entity.setDuedate(new Date(now)); 
      }
      deleteIndex(session, oldEntity);
    }
    
    insert(session, entity);

    for(IndexHandler<JobEntity> index:indexHandlers.values()){
      for(Statement st:index.getUpdateStatements(session, entity, oldEntity)){
        session.addStatement(st);
      }
    }
    
    entityCache.put(entity.getId(), entity);
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

//  private void checkPriority(JobEntity entity){
//    if(entity.getPriority()<0 || entity.getPriority()>maxPriority){
//      throw new IllegalArgumentException("Job priority out of range: "+entity.getPriority()+". Configured maximum priority is "+maxPriority);
//    }    
//  }
  
  private JobEntity getCachedEntity(JobEntity entity){
    JobEntity oldEntity = entityCache.get(entity.getId());
    if(oldEntity==null){
      throw new RuntimeException("Inconsistent state, entity needs to be loaded into command context before it can be updated.");
    }
    return oldEntity;
  }

  /*
  public static JobEntity getJobByKey(CassandraPersistenceSession session, JobEntityKey key){
    BoundStatement statement = selectStatement.bind();
    statement.setDate("shard_id", new Date(key.getShardId()));
    statement.setBool("is_locked", key.isLocked());
    statement.setDate("sort_time", new Date(key.getSortTime())); 
    statement.setString("id", key.getId()); 
    Row row = session.getSession().execute(statement).one();
    if(row != null){
      CassandraSerializer<JobEntity> serializer = CassandraPersistenceSession.getSerializer(JobEntity.class);
      return serializer.read(row); 
    }
    return null;
  }*/
  
  public static IndexHandler<JobEntity> getIndexHandler(Class<?> type){
    return indexHandlers.get(type);
  }

}
