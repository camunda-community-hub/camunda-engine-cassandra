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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExclusiveJobsByDueDateIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.ExclusiveJobsByLockExpiryIndex;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.operation.JobOperations;
import org.camunda.bpm.engine.impl.db.ListQueryParameterObject;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;

/**
 * @author Natalia Levine
 *
 * @created 16/09/2015
 */
public class SelectExclusiveJobsToExecute implements SelectListQueryHandler<JobEntity, ListQueryParameterObject> {
  public List<JobEntity> executeQuery(CassandraPersistenceSession session, ListQueryParameterObject query) {
    @SuppressWarnings("unchecked")
    Map<String, Object> params = (Map<String, Object>) query.getParameter(); 
    Date now = (Date) params.get("now");
    String pid = (String) params.get("pid");
    
    IndexHandler<JobEntity> lockedIndex = JobOperations.getIndexHandler(ExclusiveJobsByLockExpiryIndex.class);
    IndexHandler<JobEntity> index = JobOperations.getIndexHandler(ExclusiveJobsByDueDateIndex.class);
    
    Map<String, Object> indexParams = new HashMap<String, Object>();
    indexParams.put("part_id", pid);
    indexParams.put("end", now);
    List<String> keys = lockedIndex.getValues(indexParams, session, (String)null);
    keys.addAll(index.getValues(indexParams, session, (String)null));
    
    List<JobEntity> result = new ArrayList<JobEntity>();
    for(String keyStr : keys){
      //JobEntityKey key = new JobEntityKey();
      //key.fromJsonString(keyStr);
      JobEntity job = session.selectById(JobEntity.class, keyStr);
      if(job!=null){
        result.add(job);
      }
    }
    return result;
  }
}
