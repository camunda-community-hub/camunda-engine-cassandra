package org.camunda.bpm.engine.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.Message;
import org.camunda.bpm.model.bpmn.instance.Process;
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
      .setAuthorizationEnabled(false)
      .buildProcessEngine();
  }
  
  @AfterClass
  public static void stopProcessEngine() throws Exception {
      processEngine.close();
  }
  
  @Test
  public void testDeployProcess() {
    
    BpmnModelInstance model = Bpmn.createExecutableProcess("testProcess").done();
    
    Message message1 = model.newInstance(Message.class);
    message1.setName("orderCancelled");
    model.getDefinitions().addChildElement(message1);
    
    Message message2 = model.newInstance(Message.class);
    message2.setName("orderCompleted");
    model.getDefinitions().addChildElement(message2);
    
    BpmnModelInstance theProcess = model.<Process>getModelElementById("testProcess") 
      .builder()
      .startEvent()
      .parallelGateway()
        .receiveTask()
          .message(message1)
        .endEvent()
      .moveToLastGateway()
        .receiveTask()
          .message(message2)
        .endEvent()
    .done();
    
    processEngine.getRepositoryService().createDeployment()
     .addModelInstance("test.bpmn", theProcess)
     .deploy();
    
    Map<String, Object> vars= new HashMap<String, Object>();
    vars.put("testVar", "testVarValue");
    ProcessInstance processInstance = processEngine.getRuntimeService()
     .startProcessInstanceByKey("testProcess", vars);
    
    processEngine.getRuntimeService().getVariable(processInstance.getId(), "testVar");
    
    processEngine.getRuntimeService()
      .createMessageCorrelation("orderCancelled")
      .processInstanceId(processInstance.getId())
      .correlate();
  }
  
}
