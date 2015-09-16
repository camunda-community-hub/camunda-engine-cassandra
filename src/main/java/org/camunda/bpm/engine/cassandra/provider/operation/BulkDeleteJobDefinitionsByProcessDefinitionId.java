/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.camunda.bpm.engine.cassandra.provider.operation;

import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.JobDefinitionIdByProcessDefinitionIdIndex;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;

import com.datastax.driver.core.BatchStatement;

/**
 * @author Natalia Levine
 *
 * @created 17/09/2015
 */

public class BulkDeleteJobDefinitionsByProcessDefinitionId implements BulkOperationHandler {

  public void perform(CassandraPersistenceSession session, Object parameter, BatchStatement flush) {
    String procDefId = (String) parameter;

    IndexHandler<JobDefinitionEntity> index = JobDefinitionOperations.getIndexHandler(JobDefinitionIdByProcessDefinitionIdIndex.class);
    List<String> ids = index.getValues(null,session, procDefId);
    
    JobDefinitionOperations ops = (JobDefinitionOperations) session.getOperationsHandler(JobDefinitionEntity.class);
    
    for (String jobDefId:ids){
      JobDefinitionEntity def = session.selectById(JobDefinitionEntity.class, jobDefId);
      ops.delete(session, def, flush);
    }
  }
}
