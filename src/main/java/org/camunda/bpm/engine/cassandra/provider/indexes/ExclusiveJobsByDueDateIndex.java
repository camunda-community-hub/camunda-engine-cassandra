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

package org.camunda.bpm.engine.cassandra.provider.indexes;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.table.JobEntityKey;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;

import com.datastax.driver.core.Statement;

/**
 * @author Natalia Levine
 *
 * @created 16/09/2015
 */
public class ExclusiveJobsByDueDateIndex extends AbstractOrderedIndexHandler<JobEntity> {

  @Override
  protected String getIndexName() {
     return IndexNames.EXCLUSIVE_JOBS_BY_DUE_DATE;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  protected String getValue(JobEntity entity) {
    //only index unlocked exclusive entities
    if(!entity.isExclusive() || entity.getLockExpirationTime()!=null ||
        entity.getProcessInstanceId()==null){
      return null;
    }
    //JobEntityKey key = new JobEntityKey(entity);
    //return key.toJsonString();
    return entity.getId();
  }

  @Override
  protected String getPartitionId(JobEntity entity) {
    return entity.getProcessInstanceId();
  }

  @Override
  protected Date getOrderBy(JobEntity entity) {
    return entity.getDuedate();
  }
  
  @Override
  public boolean checkIndexMatch(JobEntity entity, JobEntity newEntity){
    return getPartitionId(entity).equals(getPartitionId(newEntity)) &&
        ( (getOrderBy(entity)==null && getOrderBy(newEntity)==null) ||
          (getOrderBy(entity)!=null && getOrderBy(entity).equals(getOrderBy(newEntity)) ) &&
          //return false if the locked state has changed
        ( (entity.getLockExpirationTime()==null && newEntity.getLockExpirationTime()==null) ||
          (entity.getLockExpirationTime()!=null && newEntity.getLockExpirationTime()!=null) ) );
  }
}
