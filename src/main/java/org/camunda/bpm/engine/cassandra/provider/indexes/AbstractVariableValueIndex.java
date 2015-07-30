package org.camunda.bpm.engine.cassandra.provider.indexes;

import java.util.Collections;
import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.core.variable.type.PrimitiveValueTypeImpl.BytesTypeImpl;
import org.camunda.bpm.engine.impl.core.variable.value.UntypedValueImpl;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;
import org.camunda.bpm.engine.impl.variable.serializer.TypedValueSerializer;
import org.camunda.bpm.engine.variable.value.TypedValue;

/**
 * Implements index returning executionId by variable Name and variable value  
 * All primitive type variables are supported except BytesTypeImpl.  
 *  
 * @author Natalia Levine
 *
 * @created 14/07/2015
 */

public abstract class AbstractVariableValueIndex extends AbstractIndexHandler<VariableInstanceEntity> {
  private final int LENGTH_LIMIT=256; //completely arbitrary, don't want to index on really long text variables

  @Override
  protected String getIndexValue(VariableInstanceEntity entity) {
    return createIndexValue(entity.getName(), getVariableValue(entity));
  }
  
  private String getVariableValue(VariableInstanceEntity entity){
    //TODO - if indexing by objects, byte arrays or long strings is ever required, we can add hashed index. 
    //This method would have to return hashed value. This will result in some false positives, 
    //however they will be filtered out when the data is retrieved because we always check that 
    //the object matches the index when we get data by index. For now just limit indexing to simple cases  
    
    String value=null;
    if(entity.getLongValue()!=null){
      value=entity.getLongValue().toString();
    }
    else if(entity.getDoubleValue()!=null){
      value=entity.getDoubleValue().toString();
    }
    else if(entity.getTextValue()!=null){
      value=entity.getTextValue();
    }
    //not bothering with text2
    
    if(value==null){
      return null;
    }
    if(value.length()>LENGTH_LIMIT){
      value=value.substring(0, LENGTH_LIMIT);
    }
    return value;
  }

  @Override
  protected String getIndexValue(String... indexValues) {
    if(indexValues.length != 2){
      throw new IllegalArgumentException("ExecutionIdByVariableValueIndex requires variable name and variable value");
    }
    return createIndexValue(indexValues);
  }

  @SuppressWarnings("unchecked")
  public List<String> getValuesByTypedValue(CassandraPersistenceSession cassandraPersistenceSession, String variableName, TypedValue typedValue) {
    @SuppressWarnings("rawtypes")
    TypedValueSerializer serializer = VariableInstanceEntity.getSerializers().findSerializerForValue(typedValue);

    if(typedValue instanceof UntypedValueImpl) {
      typedValue = serializer.convertToTypedValue((UntypedValueImpl) typedValue);
    }

    if(typedValue.getType().isPrimitiveValueType() && !(typedValue.getType() instanceof BytesTypeImpl)){
      VariableInstanceEntity tempEntity = new VariableInstanceEntity();
      serializer.writeValue(typedValue, tempEntity);
      return getValues(cassandraPersistenceSession, variableName, getVariableValue(tempEntity));
    }
    return Collections.emptyList();
  }
  
  @Override
  public boolean isUnique() {
    return false;
  }
}
