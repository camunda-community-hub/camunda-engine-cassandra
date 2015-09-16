/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.bpm.engine.cassandra.provider.indexes;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.table.IndexTableHandler;
import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */
public abstract class AbstractIndexHandler <T extends DbEntity> implements IndexHandler <T> {
  private final static Logger LOG = Logger.getLogger(AbstractIndexHandler.class.getName());

  protected abstract String getIndexName();
  protected abstract String getIndexValue(T entity);
  protected abstract String getValue(T entity);
  
  private static PreparedStatement insertStatement=null;
  private static PreparedStatement deleteStatement=null;
  private static PreparedStatement deleteUniqueStatement=null;
  private static PreparedStatement selectStatement=null;
    
  @Override
  public String getUniqueValue(Map<String, Object> params, CassandraPersistenceSession cassandraPersistenceSession, String ... indexValues){
    if(!isUnique()){
      throw new UnsupportedOperationException("Index "+getIndexName()+" is not unique.");
    }
    
    Session s = cassandraPersistenceSession.getSession();
    List<Row> rows = s.execute(selectStatement.bind(getIndexName(),getIndexValue(indexValues))).all();
        
    if(rows == null || rows.size()==0) {
      return null;
    }
    if(rows.size()>1){
      LOG.warning("Multiple values found for a unique index "+getIndexName()+", indexValue="+indexValues[0]);
    }
    return rows.get(0).getString("val");    
  }

  @Override
  public List<String> getValues(Map<String, Object> params, CassandraPersistenceSession cassandraPersistenceSession, String ... indexValues) {
    Session s = cassandraPersistenceSession.getSession();
    List<Row> rows = s.execute(selectStatement.bind(getIndexName(), getIndexValue(indexValues))).all();
    List<String> result = new ArrayList<String>();
    for(Row row:rows){
      result.add(row.getString("val"));
    }
    return result;
  }

  @Override
  public Statement getInsertStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity) {
    String indexValue = getIndexValue(entity);
    String value = getValue(entity);
    if(indexValue==null || value==null){
      return null;
    }
    
    return insertStatement.bind(getIndexName(), getIndexValue(entity), getValue(entity));
  }

  @Override
  public Statement getDeleteStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity) {
    String indexValue=getIndexValue(entity);
    if(indexValue==null){
      return null;
    }
    
    if(isUnique()){
      return deleteUniqueStatement.bind(getIndexName(),indexValue);
    }
    else{
      String value=getValue(entity);
      if(value==null){
        return null;
      }
      return deleteStatement.bind(getIndexName(),indexValue,value);
    }
  }

  @Override
  public List<Statement> getUpdateStatements(CassandraPersistenceSession cassandraPersistenceSession, T entity, T oldEntity){
    if(!checkIndexMatch(oldEntity, entity)){
      return Arrays.asList(getDeleteStatement(cassandraPersistenceSession,oldEntity),
            getInsertStatement(cassandraPersistenceSession,entity));  
    }
    return Collections.emptyList();
  }
 
  public boolean checkIndexMatch(T entity, String ... indexValues){
    return getIndexValue(entity).equals(getIndexValue(indexValues));
  }
 
  public boolean checkIndexMatch(T entity, T newEntity){
    return getIndexValue(entity).equals(getIndexValue(newEntity));
  }
 
  protected String getIndexValue(String... indexValues) {
    if(indexValues.length > 1){
      throw new IllegalArgumentException("This index supports only one index value.");
    }
    return IndexUtils.createIndexValue(indexValues);
  }

  public static void prepare(CassandraProcessEngineConfiguration config) {
    selectStatement = config.getSession().prepare(select("val")
        .from(IndexTableHandler.INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("idx_value", QueryBuilder.bindMarker())));
    
    insertStatement = config.getSession().prepare(insertInto(IndexTableHandler.INDEX_TABLE_NAME)
        .value("idx_name", QueryBuilder.bindMarker())
        .value("idx_value",QueryBuilder.bindMarker())
        .value("val",QueryBuilder.bindMarker()));
    
    deleteStatement = config.getSession().prepare(delete().all()
        .from(IndexTableHandler.INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("idx_value",QueryBuilder.bindMarker()))
        .and(eq("val",QueryBuilder.bindMarker())));
    
    deleteUniqueStatement = config.getSession().prepare(delete().all()
        .from(IndexTableHandler.INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("idx_value",QueryBuilder.bindMarker())));
   
  }
  
}