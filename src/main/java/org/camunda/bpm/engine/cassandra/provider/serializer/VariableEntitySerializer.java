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
    if(entity.getLongValue()!=null){
      data.setLong("long", entity.getLongValue());
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
    entity.setDoubleValue(data.getDouble("double"));
    entity.setLongValue(data.getLong("long"));
    entity.setTextValue(data.getString("text"));
    entity.setTextValue2(data.getString("text2"));
    entity.setSequenceCounter(data.getLong("sequence_counter"));
    entity.setConcurrentLocal(data.getBool("is_concurrent_local"));
    return entity;
  }

}
