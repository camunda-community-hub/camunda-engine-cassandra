package org.camunda.bpm.engine.cassandra.provider.indexes;

import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public class ExecutionIdByEventTypeAndNameIndex extends AbstractIndexHandler<EventSubscriptionEntity> {

  @Override
  protected String getTableName() {
     return ProcessInstanceTableHandler.INDEX_TABLE_NAME;
  }

  @Override
  protected String getIndexName() {
     return IndexNames.EXECUTION_ID_BY_EVENT_NAME;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  protected String getIndexValue(String... indexValues) {
    if(indexValues.length!=2){
      throw new IllegalArgumentException("ExecutionIdByEventTypeAndNameIndex requires event type and event name");
    }
    
    return createIndexValue(indexValues);
  }

  @Override
  protected String getIndexValue(EventSubscriptionEntity entity) {
    return createIndexValue(entity.getEventType(), entity.getEventName());
  }

  @Override
  protected String getValue(EventSubscriptionEntity entity) {
    return entity.getExecutionId();
  }

}
