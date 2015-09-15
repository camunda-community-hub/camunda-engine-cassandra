package org.camunda.bpm.engine.cassandra.provider.serializer;

import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;

public class VariableEntitySerializer implements CassandraSerializer<VariableInstanceEntity> {

  public void write(SettableData<?> data, VariableInstanceEntity entity) {
    data.setString("id", entity.getId())
    .setString("type", entity.getSerializerName())
    .setString("name", entity.getName())
    .setString("execution_id", entity.getExecutionId())
    .setString("proc_inst_id", entity.getProcessInstanceId())
    .setString("case_execution_id", entity.getCaseExecutionId())
    .setString("case_inst_id", entity.getCaseInstanceId())
    .setString("task_id", entity.getTaskId())
    .setString("bytearray_id", entity.getByteArrayValueId())
    .setString("text", entity.getTextValue())
    .setString("text2", entity.getTextValue2())
    .setLong("sequence_counter", entity.getSequenceCounter())
    .setBool("is_concurrent_local", entity.isConcurrentLocal());

    //numbers break with NULLS
    if(entity.getDoubleValue()!=null){
      data.setDouble("double", entity.getDoubleValue());
    }
    else{
      data.setToNull("double");
    }
    if(entity.getLongValue()!=null){
      data.setLong("long", entity.getLongValue());
    }
    else{
      data.setToNull("long");
    }
  }
  
  public VariableInstanceEntity read(GettableData data) {
    VariableInstanceEntity entity = new VariableInstanceEntity();
    entity.setId(data.getString("id"));
    entity.setSerializerName(data.getString("type"));
    entity.setName(data.getString("name"));
    entity.setExecutionId(data.getString("execution_id"));
    entity.setProcessInstanceId(data.getString("proc_inst_id"));
    entity.setCaseExecutionId(data.getString("case_execution_id"));
    entity.setCaseInstanceId(data.getString("case_inst_id"));
    entity.setTaskId(data.getString("task_id"));
    entity.setByteArrayValueId(data.getString("bytearray_id"));
    if(!data.isNull("double")){
      entity.setDoubleValue(data.getDouble("double"));
    }
    if(!data.isNull("long")){
      entity.setLongValue(data.getLong("long"));
    }
    entity.setTextValue(data.getString("text"));
    entity.setTextValue2(data.getString("text2"));
    entity.setSequenceCounter(data.getLong("sequence_counter"));
    entity.setConcurrentLocal(data.getBool("is_concurrent_local"));
    return entity;
  }

  public VariableInstanceEntity copy(VariableInstanceEntity data) {
    VariableInstanceEntity entity = new VariableInstanceEntity();
    entity.setId(data.getId());
    entity.setSerializerName(data.getSerializerName());
    entity.setName(data.getName());
    entity.setExecutionId(data.getExecutionId());
    entity.setProcessInstanceId(data.getProcessInstanceId());
    entity.setCaseExecutionId(data.getCaseExecutionId());
    entity.setCaseInstanceId(data.getCaseInstanceId());
    entity.setTaskId(data.getTaskId());
    entity.setByteArrayValueId(data.getByteArrayValueId());
    entity.setDoubleValue(data.getDoubleValue());
    entity.setLongValue(data.getLongValue());
    entity.setTextValue(data.getTextValue());
    entity.setTextValue2(data.getTextValue2());
    entity.setSequenceCounter(data.getSequenceCounter());
    entity.setConcurrentLocal(data.isConcurrentLocal());
    return entity;
  }

}
