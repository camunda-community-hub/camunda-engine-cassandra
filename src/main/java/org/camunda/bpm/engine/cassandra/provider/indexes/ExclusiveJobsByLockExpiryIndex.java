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

import java.util.Date;

import org.camunda.bpm.engine.cassandra.provider.table.JobEntityKey;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;

/**
 * @author Natalia Levine
 *
 * @created 16/09/2015
 */
public class ExclusiveJobsByLockExpiryIndex extends AbstractOrderedIndexHandler<JobEntity> {

  @Override
  protected String getIndexName() {
     return IndexNames.EXCLUSIVE_JOBS_BY_LOCK_TIME;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  protected String getValue(JobEntity entity) {
    //only index locked exclusive entities
    if(!entity.isExclusive() || entity.getLockExpirationTime()==null ||
        entity.getProcessInstanceId()==null){
      return null;
    }
   // JobEntityKey key = new JobEntityKey(entity);
   // return key.toJsonString();
    return entity.getId();
  }

  @Override
  protected String getPartitionId(JobEntity entity) {
    return entity.getProcessInstanceId();
  }

  @Override
  protected Date getOrderBy(JobEntity entity) {
    return entity.getLockExpirationTime();
  }
}
