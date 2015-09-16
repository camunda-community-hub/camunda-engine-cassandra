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
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.camunda.bpm.engine.cassandra.provider.table.OrderedIndexTableHandler.INDEX_TABLE_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * @author Natalia
 *
 * @created 16/09/2015
 */
public abstract class AbstractOrderedIndexHandler<T extends DbEntity> implements IndexHandler<T> {
  private final static Logger LOG = Logger.getLogger(AbstractOrderedIndexHandler.class.getName());

  protected abstract String getIndexName();
  protected abstract String getPartitionId(T entity);
  protected abstract Date getOrderBy(T entity);
  protected abstract String getValue(T entity);
  
  private static PreparedStatement insertStatement=null;
  private static PreparedStatement deleteStatement=null;
  private static PreparedStatement deleteUniqueStatement=null;
  private static PreparedStatement selectStatement=null;
  private static PreparedStatement selectLessThanStatement=null;
  private static PreparedStatement selectGreaterThanStatement=null;
  private static PreparedStatement selectBetweenStatement=null;
    
  public static void prepare(CassandraProcessEngineConfiguration config) {
    selectLessThanStatement = config.getSession().prepare(select("val")
        .from(INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("part_id", QueryBuilder.bindMarker()))
        .and(lt("order_by", QueryBuilder.bindMarker())));
    
    selectGreaterThanStatement = config.getSession().prepare(select("val")
        .from(INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("part_id", QueryBuilder.bindMarker()))
        .and(gt("order_by", QueryBuilder.bindMarker())));
    
    selectBetweenStatement = config.getSession().prepare(select("val")
        .from(INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("part_id", QueryBuilder.bindMarker()))
        .and(gt("order_by", QueryBuilder.bindMarker()))
        .and(lt("order_by", QueryBuilder.bindMarker())));

    selectStatement = config.getSession().prepare(select("val")
        .from(INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("part_id", QueryBuilder.bindMarker()))
        .and(eq("order_by", QueryBuilder.bindMarker())));
    
    insertStatement = config.getSession().prepare(insertInto(INDEX_TABLE_NAME)
        .value("idx_name", QueryBuilder.bindMarker())
        .value("part_id", QueryBuilder.bindMarker())
        .value("order_by", QueryBuilder.bindMarker())
        .value("val",QueryBuilder.bindMarker()));
    
    deleteStatement = config.getSession().prepare(delete().all()
        .from(INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("part_id", QueryBuilder.bindMarker()))
        .and(eq("order_by", QueryBuilder.bindMarker()))
        .and(eq("val",QueryBuilder.bindMarker())));
    
    deleteUniqueStatement = config.getSession().prepare(delete().all()
        .from(INDEX_TABLE_NAME)
        .where(eq("idx_name", QueryBuilder.bindMarker()))
        .and(eq("part_id", QueryBuilder.bindMarker()))
        .and(eq("order_by", QueryBuilder.bindMarker())));
  }

  @Override
  public Statement getInsertStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity) {
    String value = getValue(entity);
    if(value==null){
      return null;
    }
    
    return insertStatement.bind(getIndexName(), getPartitionId(entity), getOrderBy(entity), getValue(entity));
  }

  @Override
  public Statement getDeleteStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity) {
    if(isUnique()){
      return deleteUniqueStatement.bind(getIndexName(), getPartitionId(entity), getOrderBy(entity));
    }
    else{
      String value=getValue(entity);
      if(value==null){
        return null;
      }
      return deleteStatement.bind(getIndexName(), getPartitionId(entity), getOrderBy(entity),value);
    }
  }

  @Override
  public List<Statement> getUpdateStatements(CassandraPersistenceSession cassandraPersistenceSession, T entity, T oldEntity) {
    if(!checkIndexMatch(oldEntity, entity)){
      return Arrays.asList(getDeleteStatement(cassandraPersistenceSession,oldEntity),
            getInsertStatement(cassandraPersistenceSession,entity));  
    }
    return Collections.emptyList();
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  public boolean checkIndexMatch(T entity, T newEntity){
    return getPartitionId(entity).equals(getPartitionId(newEntity)) &&
        ((getOrderBy(entity)==null && getOrderBy(newEntity)==null) ||
        (getOrderBy(entity)!=null && getOrderBy(entity).equals(getOrderBy(newEntity))));
  }

  @Override
  public String getUniqueValue(Map<String, Object> params, CassandraPersistenceSession cassandraPersistenceSession, String ... indexValues){
    if(!isUnique()){
      throw new UnsupportedOperationException("Index "+getIndexName()+" is not unique.");
    }
    
    if(params == null || !params.containsKey("part_id") || !params.containsKey("order_by")){
      throw new IllegalArgumentException("Partition ID and ordering value is required for ordered index "+getIndexName());
    }

    Session s = cassandraPersistenceSession.getSession();
    String partId=(String) params.get("part_id");
    Date orderBy=(Date) params.get("order_by");
    List<Row> rows = s.execute(selectStatement.bind(getIndexName(), partId, orderBy)).all();
        
    if(rows == null || rows.size()==0) {
      return null;
    }
    if(rows.size()>1){
      LOG.warning("Multiple values found for a unique index "+getIndexName());
    }
    return rows.get(0).getString("val");    
  }

  @Override
  public List<String> getValues(Map<String, Object> params, CassandraPersistenceSession cassandraPersistenceSession, String ... indexValues) {
    if(params == null || !params.containsKey("part_id") || 
        !(params.containsKey("order_by") || params.containsKey("start") || params.containsKey("end"))){
      throw new IllegalArgumentException("Partition ID and ordering value is required for ordered index "+getIndexName());
    }

    Session s = cassandraPersistenceSession.getSession();
    String partId=(String) params.get("part_id");
    Date orderBy=(Date) params.get("order_by");
    Date start=(Date) params.get("start");
    Date end=(Date) params.get("end");
    
    List<Row> rows = null;
    if(orderBy!=null){
      //select exact
      rows = s.execute(selectStatement.bind(getIndexName(), partId, orderBy)).all();
    }
    else if(start!=null && end!=null){
      //bounded range
      rows = s.execute(selectBetweenStatement.bind(getIndexName(), partId, start, end)).all();      
    }
    else if(start!=null){
      rows = s.execute(selectGreaterThanStatement.bind(getIndexName(), partId, start)).all();            
    }
    else if(end!=null){
      rows = s.execute(selectLessThanStatement.bind(getIndexName(), partId, end)).all();            
    }
    
    List<String> result = new ArrayList<String>();
    for(Row row:rows){
      result.add(row.getString("val"));
    }
    return result;
  }
}
