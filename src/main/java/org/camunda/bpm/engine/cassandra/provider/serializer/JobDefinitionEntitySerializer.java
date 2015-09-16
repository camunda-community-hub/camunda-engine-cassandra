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
package org.camunda.bpm.engine.cassandra.provider.serializer;

import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

/**
 * @author Natalia Levine
 *
 * @created 15/09/2015
 */

public class JobDefinitionEntitySerializer implements CassandraSerializer<JobDefinitionEntity> {

  @Override
  public void write(SettableData<?> data, JobDefinitionEntity entity) {
    data.setString("id", entity.getId());
    data.setString("proc_def_id", entity.getProcessDefinitionId());
    data.setString("proc_def_key", entity.getProcessDefinitionKey());
    data.setString("act_id", entity.getActivityId());
    data.setString("type", entity.getJobType());
    data.setString("config", entity.getJobConfiguration());
    if(entity.getOverridingJobPriority()!=null){
      data.setInt("priority", entity.getOverridingJobPriority());
    }
    else{
      data.setToNull("priority");
    }
    data.setInt("suspension_state", entity.getSuspensionState());
    data.setInt("revision", entity.getRevision());
  }

  @Override
  public JobDefinitionEntity read(GettableData data) {
    JobDefinitionEntity entity = new JobDefinitionEntity();
    entity.setId(data.getString("id"));
    entity.setProcessDefinitionId(data.getString("proc_def_id"));
    entity.setProcessDefinitionKey(data.getString("proc_def_key"));
    entity.setActivityId(data.getString("act_id"));
    entity.setJobType(data.getString("type"));
    entity.setJobConfiguration(data.getString("config"));
    if(!data.isNull("priority")){
      entity.setJobPriority(data.getInt("priority"));
    }
    entity.setSuspensionState(data.getInt("suspension_state"));
    entity.setRevision(data.getInt("revision"));
    return entity;
  }

  @Override
  public JobDefinitionEntity copy(JobDefinitionEntity data) {
    JobDefinitionEntity entity = new JobDefinitionEntity();
    entity.setId(data.getId());
    entity.setProcessDefinitionId(data.getProcessDefinitionId());
    entity.setProcessDefinitionKey(data.getProcessDefinitionKey());
    entity.setActivityId(data.getActivityId());
    entity.setJobType(data.getJobType());
    entity.setJobConfiguration(data.getJobConfiguration());
    entity.setJobPriority(data.getOverridingJobPriority());
    entity.setSuspensionState(data.getSuspensionState());
    entity.setRevision(data.getRevision());
    return entity;
  }

}
