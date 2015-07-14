package org.camunda.bpm.engine.cassandra.provider.indexes;

import org.camunda.bpm.engine.cassandra.provider.table.ProcessInstanceTableHandler;
import org.camunda.bpm.engine.impl.persistence.entity.EventSubscriptionEntity;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public class ProcessIdByEventSubscriptionIdIndex extends AbstractIndexHandler<EventSubscriptionEntity> {

  @Override
  protected String getTableName() {
    return ProcessInstanceTableHandler.INDEX_TABLE_NAME;
  }

  @Override
  protected String getIndexName() {
    return IndexNames.PROCESS_ID_BY_EVENT_SUBSCRIPTION_ID;
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  protected String getIndexValue(EventSubscriptionEntity entity) {
    return entity.getId();
  }

  @Override
  protected String getValue(EventSubscriptionEntity entity) {
    return entity.getProcessInstanceId();
  }

}
