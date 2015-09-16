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
package org.camunda.bpm.engine.cassandra.provider.query;

import static org.camunda.bpm.engine.cassandra.provider.table.JobTableHandler.JOB_INDEX_TABLE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexUtils;
import org.camunda.bpm.engine.impl.db.ListQueryParameterObject;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author Natalia Levine
 *
 * @created 15/09/2015
 */
public class SelectNextJobsToExecute implements SelectListQueryHandler<JobEntity, ListQueryParameterObject> {
  private final static String SELECT = "SELECT id FROM "+JOB_INDEX_TABLE+" WHERE "
      + "shard_id = ? AND "
      + "is_locked = ? AND "
      + "sort_time < ? "
      + "LIMIT ?;";

  private final static String SELECT_ACTIVE = "SELECT id FROM "+JOB_INDEX_TABLE+" WHERE "
      + "shard_id = ? AND "
      + "is_locked = ? "
      + "LIMIT ?;";

  private static PreparedStatement selectActiveStatement=null;
  private static PreparedStatement selectStatement=null;

  protected static int shardSizeMillis; //size of the job shard
  protected static int shardInitNumber; //how far to go back to find active shards on start-up 
  //protected static int maxPriority; //maximum possible priority
  
  protected static List<Long> activeShards = Collections.synchronizedList(new LinkedList<Long>());
  protected static List<Long> activeLockedShards = Collections.synchronizedList(new LinkedList<Long>());
  
  public static void prepare(CassandraProcessEngineConfiguration config) {
    selectActiveStatement = config.getSession().prepare(SELECT_ACTIVE);
    selectStatement = config.getSession().prepare(SELECT);
    
    shardSizeMillis = config.getJobShardSizeHours()*3600*1000;
    shardInitNumber=config.getJobShardInitNumber();
    //maxPriority=config.getMaxPriority();
    
    long currentShard = IndexUtils.calculateShard(System.currentTimeMillis(), shardSizeMillis); 
    //NOTE - locked indexes are sorted and sharded by lock expiry date and
    //unlocked indexes are sorted and sharded by the job due date
    for(int i=0;i<shardInitNumber;i++){
      currentShard = IndexUtils.calculateShard(currentShard-1, shardSizeMillis);//start with the current shard - 1 and go backwards
     // for(int j=0; j<=maxPriority;j++){
     //   if(checkActive(currentShard, false, j, config.getSession()) && activeShards.get(0)!=currentShard){
     //     activeShards.add(0, currentShard); 
     //   }
     // }
      
      if(checkActive(currentShard, false, config.getSession())){
        activeShards.add(0, currentShard); 
      }
      if(checkActive(currentShard, true, config.getSession())){
        activeLockedShards.add(0, currentShard); 
      }
    }
  }

  @SuppressWarnings("unchecked")
  public List<JobEntity> executeQuery(CassandraPersistenceSession session, ListQueryParameterObject query) {
    long currentShard = IndexUtils.calculateShard(System.currentTimeMillis(),shardSizeMillis); 
     
    Map<String, Object> params = (Map<String, Object>) query.getParameter();
    Date now=(Date) params.get("now");
    int maxResults=query.getMaxResults();
    List<JobEntity> result=new ArrayList<JobEntity>();
    //first look for all locked jobs where the lock has expired
    queryShards(activeLockedShards, result, now, maxResults, session, currentShard, true);
    //now look for remaining jobs by due date
    if(result.size()<maxResults){
      queryShards(activeShards, result, now, maxResults, session, currentShard, false);      
    }
    return result;
  }

  private void queryShards(List<Long> shards, List<JobEntity> result, Date now, int maxResults, CassandraPersistenceSession session, long currentShard, boolean locked ){
    List<Long> localShards = new ArrayList<Long>();
    synchronized(shards){
      //make sure the current slice is always marked as active
      //assuming that this query is executed at least once in each shard time slice
      if(shards.isEmpty()||shards.get(shards.size()-1)<currentShard){
        shards.add(currentShard);
      }
      localShards.addAll(shards);  
    }
    
    long prevShard = IndexUtils.calculateShard(currentShard-1, shardSizeMillis);
    
    TOPLOOP: for(Long shard: localShards){
      BoundStatement statement = selectStatement.bind();
      statement.setDate("shard_id", new Date(shard));
      statement.setBool("is_locked", locked);
      statement.setDate(2, now); //date in the past
      statement.setInt(3, maxResults); //limit
      List<Row> rows = session.getSession().execute(statement).all();
      for(Row row:rows){
        JobEntity job= session.selectById(JobEntity.class, row.getString("id"));
        if(job!=null){
          result.add(job);
        }
        
        if(result.size()>=maxResults){
          break TOPLOOP;
        }
      }
      if(rows.isEmpty() && shard < prevShard){ //keep the last 2 shards active
        if(!checkActive(shard, locked, session.getSession())){
          shards.remove(shard);
        }
      }
    }
  }

  private static boolean checkActive(long shard, boolean locked, Session session){
    BoundStatement statement = selectActiveStatement.bind();
    statement.setDate("shard_id", new Date(shard));
    statement.setBool("is_locked", locked);
    statement.setInt(2, 1); //limit, just get one
    Row row = session.execute(statement).one();
    return row!=null;
  }
}
