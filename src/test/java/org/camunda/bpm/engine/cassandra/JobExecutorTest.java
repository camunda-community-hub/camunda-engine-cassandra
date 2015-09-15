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

package org.camunda.bpm.engine.cassandra;

import org.camunda.bpm.engine.impl.jobexecutor.JobExecutor;
import org.camunda.bpm.engine.impl.test.PluggableProcessEngineTestCase;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.Deployment;

public class JobExecutorTest extends PluggableProcessEngineTestCase {

  @Deployment(resources = {"org/camunda/bpm/engine/cassandra/asynch-test.bpmn"})
  public void testParallelExecution() throws InterruptedException {
	  JobExecutor jobExecutor = processEngineConfiguration.getJobExecutor();
	  try{
		jobExecutor.start();
	    ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("test");
	    waitForJobExecutorToProcessAllJobs(20000);
	  }
	  finally 
	  {
		  jobExecutor.shutdown();
	  }

  } 
}
