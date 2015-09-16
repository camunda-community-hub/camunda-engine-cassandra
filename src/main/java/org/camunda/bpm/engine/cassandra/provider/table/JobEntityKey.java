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

package org.camunda.bpm.engine.cassandra.provider.table;

import org.camunda.bpm.engine.cassandra.provider.indexes.IndexUtils;
import org.camunda.bpm.engine.cassandra.provider.operation.JobOperations;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.util.json.JSONObject;


/**
 * @author Natalia Levine
 *
 * @created 16/09/2015
 */

public class JobEntityKey {
  private long shardId;
  private boolean isLocked;
  private long sortTime;
  private String id;
  
  public JobEntityKey(JobEntity entity, int shardSizeMillis){
    isLocked = entity.getLockExpirationTime()!=null;
    shardId=IndexUtils.calculateShard(isLocked ? entity.getLockExpirationTime().getTime() : entity.getDuedate().getTime(), shardSizeMillis);
    sortTime = isLocked ? entity.getLockExpirationTime().getTime() : entity.getDuedate().getTime();
    id = entity.getId();
  }

  public JobEntityKey(long shardId, boolean isLocked, long sortTime, String id) {
    super();
    this.shardId = shardId;
    this.isLocked = isLocked;
    this.sortTime = sortTime;
    this.id = id;
  }

  @Override
  public boolean equals(Object obj){
    if( !(obj instanceof JobEntityKey) ){
      return false;
    }
    JobEntityKey other = (JobEntityKey) obj;
    
    return shardId == other.shardId 
        && isLocked == other.isLocked 
        && sortTime == other.sortTime
        && ((id==null && other.id == null) || (id!=null && id.equals(other.id)));
  }
 
  public String toJsonString(){
    JSONObject value = new JSONObject();
    value.put("shard_id", shardId);
    value.put("is_locked", isLocked);
    value.put("sort_time", sortTime);
    value.put("id", id);
    return value.toString();
  }
 
  public void fromJsonString(String str){
    JSONObject value = new JSONObject(str);
    if(value.has("shard_id")){
      shardId = value.getLong("shard_id");
    }
    if(value.has("is_locked")){
      isLocked = value.getBoolean("is_locked");
    }
    if(value.has("sort_time")){
      sortTime = value.getLong("sort_time");
    }
    if(value.has("id")){
      id = value.getString("id");
    }
  }

  public long getShardId() {
    return shardId;
  }

  public void setShardId(long shardId) {
    this.shardId = shardId;
  }

  public boolean isLocked() {
    return isLocked;
  }

  public void setLocked(boolean isLocked) {
    this.isLocked = isLocked;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getSortTime() {
    return sortTime;
  }

  public void setSortTime(long sortTime) {
    this.sortTime = sortTime;
  }
}
