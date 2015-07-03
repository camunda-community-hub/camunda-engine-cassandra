package org.camunda.bpm.engine.cassandra;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class InitProcessEngineTest {
  
  protected static ProcessEngine processEngine;
  
  @BeforeClass
  public static void initCassandra() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
    processEngine = new CassandraProcessEngineConfiguration()
      .setCassandraContactPoint("127.0.0.1")
      .setDatabaseSchemaUpdate("true")
      .setHistory(ProcessEngineConfiguration.HISTORY_NONE)
      .setMetricsEnabled(false)
      .buildProcessEngine();
  }
  
  @AfterClass
  public static void cleanCassandra() throws Exception {
//    try {
      processEngine.close();
//    }
//    finally {
//      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
//    }
    
  }
  
  @Test
  public void testDeployProcess() {
    
    BpmnModelInstance theProcess = Bpmn.createExecutableProcess("testProcess")
      .startEvent()
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
