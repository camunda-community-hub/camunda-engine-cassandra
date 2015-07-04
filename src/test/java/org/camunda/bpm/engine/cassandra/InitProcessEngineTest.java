package org.camunda.bpm.engine.cassandra;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class InitProcessEngineTest {
  
  protected static ProcessEngine processEngine;
  
  @BeforeClass
  public static void initProcessEngine() throws Exception {
    processEngine = new CassandraProcessEngineConfiguration()
      .setCassandraContactPoint("127.0.0.1")
      .setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_CREATE_DROP)
      .setHistory(ProcessEngineConfiguration.HISTORY_NONE)
      .setMetricsEnabled(false)
      .buildProcessEngine();
  }
  
  @AfterClass
  public static void stopProcessEngine() throws Exception {
      processEngine.close();
  }
  
  @Test
  public void testDeployProcess() {
    
    BpmnModelInstance theProcess = Bpmn.createExecutableProcess("testProcess")
      .startEvent()
      .parallelGateway()
        .userTask()
        .endEvent()
      .moveToLastGateway()
        .userTask()
        .endEvent()
    .done();
    
    processEngine.getRepositoryService().createDeployment()
     .addModelInstance("test.bpmn", theProcess)
     .deploy();
    
    processEngine.getRuntimeService()
     .startProcessInstanceByKey("testProcess");
    
  }
  
}
