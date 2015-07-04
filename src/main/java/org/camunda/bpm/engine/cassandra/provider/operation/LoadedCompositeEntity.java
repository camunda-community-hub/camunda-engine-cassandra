package org.camunda.bpm.engine.cassandra.provider.operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.impl.db.DbEntity;

public class LoadedCompositeEntity {
  
  protected DbEntity primary;
  
  protected Map<String, Map<String, DbEntity>> embeddedEntities = new HashMap<String, Map<String,DbEntity>>();
  
  public Map<String, DbEntity> get(String name) {
    return embeddedEntities.get(name);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void put(String name,   Map entities) {
    embeddedEntities.put(name, entities);
  }
  
  public DbEntity getMainEntity() {
    return primary;
  }
  
  public void setMainEntity(DbEntity mainEntity) {
    this.primary = mainEntity;
  }
  
  public Map<String, Map<String, DbEntity>> getEmbeddedEntities() {
    return embeddedEntities;
  }
}
