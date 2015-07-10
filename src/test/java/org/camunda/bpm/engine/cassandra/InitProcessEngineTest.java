package org.camunda.bpm.engine.cassandra;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.impl.test.PluggableProcessEngineTestCase;
import org.camunda.bpm.engine.repository.Deployment;
import org.camunda.bpm.engine.runtime.Execution;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.Message;
import org.camunda.bpm.model.bpmn.instance.Process;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class InitProcessEngineTest extends PluggableProcessEngineTestCase{
  
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
    
    Deployment deployment = processEngine.getRepositoryService().createDeployment()
     .addModelInstance("test.bpmn", theProcess)
     .deploy();
    
    Map<String, Object> vars= new HashMap<String, Object>();
    vars.put("testVar", "testVarValue");
    ProcessInstance processInstance = processEngine.getRuntimeService()
     .startProcessInstanceByKey("testProcess", vars);

    String varValue = (String) processEngine.getRuntimeService().getVariable(processInstance.getId(), "testVar");
    assertEquals("testVarValue",varValue);
    
    processEngine.getRuntimeService().setVariable(processInstance.getId(), "testVar2", "testVarValue2");

    String varValue2 = (String) processEngine.getRuntimeService().getVariable(processInstance.getId(), "testVar2");
    assertEquals("testVarValue2",varValue2);
   /* List<Execution> executions=processEngine.getRuntimeService()
    		.createExecutionQuery()
    		.processInstanceId(processInstance.getId())
    		.list();
    
    String executionId = null;
    for(Execution execution:executions){
    	if(!execution.getId().equals(processInstance.getId())){
    		executionId = execution.getId();
    	    processEngine.getRuntimeService().setVariableLocal(executionId, "testVarLocal", "testVarLocalValue"); 
    	    break;
    	}
    }
    assertNotNull(executionId);
    String varValueLocal = (String) processEngine.getRuntimeService().getVariable(executionId, "testVarLocal");
    assertEquals("testVarLocalValue",varValueLocal);*/
    
    processEngine.getRuntimeService()
      .createMessageCorrelation("orderCancelled")
      .processInstanceId(processInstance.getId())
      .correlate();

    processEngine.getRuntimeService()
    .createMessageCorrelation("orderCompleted")
    .processInstanceId(processInstance.getId())
    .correlate();

    assertProcessEnded(processInstance.getId());
    
    processEngine.getRepositoryService().deleteDeployment(deployment.getId(), true);

}
  
}
