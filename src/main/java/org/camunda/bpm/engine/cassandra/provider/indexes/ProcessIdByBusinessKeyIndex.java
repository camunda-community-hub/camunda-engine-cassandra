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

import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;

/**
 * This index is only created for process instances, it is not created for any other executions
 * 
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public class ProcessIdByBusinessKeyIndex extends AbstractIndexHandler<ExecutionEntity> {

  @Override
  protected String getIndexName() {
    return IndexNames.PROCESS_ID_BY_BUSINESS_KEY;
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  protected String getIndexValue(ExecutionEntity entity) {
    if(entity.isProcessInstanceExecution()){
      return entity.getBusinessKey();
    }
    return null;
  }

  @Override
  protected String getValue(ExecutionEntity entity) {
    if(entity.isProcessInstanceExecution()){
      return entity.getProcessInstanceId();
    }
    return null;
  }

}
