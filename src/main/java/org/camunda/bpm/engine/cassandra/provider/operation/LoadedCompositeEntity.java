package org.camunda.bpm.engine.cassandra.provider.operation;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.impl.db.DbEntity;

public class LoadedCompositeEntity {
  
  protected DbEntity primary;
  
  protected Map<String, Map<String, ? extends DbEntity>> embeddedEntities = new HashMap<String, Map<String, ? extends DbEntity>>();
  
  public Map<String, ? extends DbEntity> get(String name) {
    return embeddedEntities.get(name);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void put(String name,   Map entities) {
    embeddedEntities.put(name, entities);
  }
  
  public DbEntity getPrimaryEntity() {
    return primary;
  }
  
  public void setMainEntity(DbEntity mainEntity) {
    this.primary = mainEntity;
  }
  
  public Map<String, Map<String, ? extends DbEntity>> getEmbeddedEntities() {
    return embeddedEntities;
  }
}
