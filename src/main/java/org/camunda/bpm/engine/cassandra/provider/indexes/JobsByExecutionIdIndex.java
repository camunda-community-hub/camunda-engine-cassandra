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

import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;

/**
 * @author Natalia Levine
 *
 * @created 16/09/2015
 */
public class JobsByExecutionIdIndex extends AbstractIndexHandler<JobEntity> {

  @Override
  protected String getIndexName() {
     return IndexNames.JOBS_BY_EXECUTION_ID;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  protected String getIndexValue(JobEntity entity) {
    return entity.getExecutionId();
  }

  @Override
  protected String getValue(JobEntity entity) {
    //JobEntityKey key = new JobEntityKey(entity);
    //return key.toJsonString();
    return entity.getId();
  }

}
