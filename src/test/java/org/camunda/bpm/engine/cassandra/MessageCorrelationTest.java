package org.camunda.bpm.engine.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.impl.test.PluggableProcessEngineTestCase;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.Deployment;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public class MessageCorrelationTest extends PluggableProcessEngineTestCase {
  @Deployment(resources = {"org/camunda/bpm/engine/cassandra/example-sequence.bpmn"})
  public void testCorrelateByMessageName() {
    ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("Process_1");
    assertProcessNotEnded(processInstance.getId());

    //correlate by message name
    runtimeService.createMessageCorrelation("Message-Request")
      .correlate();
    assertProcessNotEnded(processInstance.getId());

    //correlate by message name and process id
    runtimeService.createMessageCorrelation("Message-Approve")
      .processInstanceId(processInstance.getId())
      .correlate();
    assertProcessEnded(processInstance.getId());
  }

  @Deployment(resources = {"org/camunda/bpm/engine/cassandra/example-sequence.bpmn"})
  public void testCorrelateByBusinessKey() {
    ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("Process_1", "business key");
    assertProcessNotEnded(processInstance.getId());
    
    runtimeService.createMessageCorrelation("Message-Request")
      .processInstanceBusinessKey(processInstance.getBusinessKey())
      .correlate();
    assertProcessNotEnded(processInstance.getId());

    runtimeService.createMessageCorrelation(null)
    .processInstanceBusinessKey(processInstance.getBusinessKey())
    .correlateAll();
    assertProcessEnded(processInstance.getId());
  }

  @Deployment(resources = {"org/camunda/bpm/engine/cassandra/example-sequence.bpmn"})
  public void testCorrelateByVariables() {
    Map<String, Object> vars= new HashMap<String, Object>();
    vars.put("testString", "testVarString");
    vars.put("testBoolean", new Boolean(true));
    vars.put("testLong", new Long(15));

    ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("Process_1", vars);
    assertProcessNotEnded(processInstance.getId());
    
    runtimeService.createMessageCorrelation("Message-Request")
      .processInstanceVariableEquals("testString", "testVarString")
      .correlate();
    assertProcessNotEnded(processInstance.getId());

    runtimeService.createMessageCorrelation(null)
      .processInstanceVariableEquals("testString", "testVarString")
      .correlate();
    assertProcessEnded(processInstance.getId());
    
  /*  runtimeService.createMessageCorrelation("stop")
    .processInstanceVariableEquals(...)
    .processInstanceBusinessKey(...)
    .correlateAll();*/
  }
}
