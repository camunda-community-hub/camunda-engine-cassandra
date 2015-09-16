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

import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;

/**
 * @author Natalia Levine
 *
 * @created 17/09/2015
 */
public class JobDefinitionIdByProcessDefinitionIdIndex extends AbstractIndexHandler<JobDefinitionEntity> {

  @Override
  protected String getIndexName() {
    return IndexNames.JOB_DEF_ID_BY_PROC_DEF_ID;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  protected String getIndexValue(JobDefinitionEntity entity) {
    return entity.getProcessDefinitionId();
  }

  @Override
  protected String getValue(JobDefinitionEntity entity) {
    return entity.getId();
  }

}
