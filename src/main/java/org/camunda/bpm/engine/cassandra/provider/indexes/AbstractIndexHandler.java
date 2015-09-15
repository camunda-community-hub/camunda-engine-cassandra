package org.camunda.bpm.engine.cassandra.provider.indexes;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

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
  private static boolean isInitialized=false;
    
  @Override
  public String getUniqueValue(CassandraPersistenceSession cassandraPersistenceSession, String ... indexValues){
    if(!isUnique()){
      throw new UnsupportedOperationException("Index "+getIndexName()+" is not unique.");
    }
    
    Session s = cassandraPersistenceSession.getSession();
    ensurePrepared(s);
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
  public List<String> getValues(CassandraPersistenceSession cassandraPersistenceSession, String ... indexValues) {
    Session s = cassandraPersistenceSession.getSession();
    ensurePrepared(s);
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
    
    ensurePrepared(cassandraPersistenceSession.getSession());   
    return insertStatement.bind(getIndexName(), getIndexValue(entity), getValue(entity));
  }

  @Override
  public Statement getDeleteStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity) {
    String indexValue=getIndexValue(entity);
    if(indexValue==null){
      return null;
    }
    
    ensurePrepared(cassandraPersistenceSession.getSession());

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
  
  public boolean checkIndexMatch(T entity, String ... indexValues){
    return getIndexValue(entity).equals(getIndexValue(indexValues));
  }
 
  public boolean checkIndexMatch(T entity, T newEntity){
    return getIndexValue(entity).equals(getIndexValue(newEntity));
  }
 
  public static Set<String> crossCheckIndexes(Set<String> set1, Set<String> set2){
    List<Set<String>> list=new ArrayList<Set<String>>(2);
    list.add(set1);
    list.add(set2);
    return crossCheckIndexes(list);
  }
  
  public static Set<String> crossCheckIndexes(List<Set<String>> sets){
    HashSet<String> result = new HashSet<String>();
    if(sets==null || sets.size()==0){
      return result;
    }
    if(sets.size()==1){
      return sets.get(0);
    }
    
    int minSize=sets.get(0).size();
    int index=0;
    for(int i=1;i<sets.size();i++){
      if(sets.get(i).size()<minSize){
        minSize=sets.get(i).size();
        index=i;
      }
    }

    for(String key: sets.get(index)){
      boolean match=true;
      for(int i=0; i < sets.size() && i != index; i++){
        if(!sets.get(i).contains(key)){
          match=false;
          break;
        }
      }
      if(match){
        result.add(key);
      }
    }
    
    return result;
  }

  
  protected String getIndexValue(String... indexValues) {
    if(indexValues.length > 1){
      throw new IllegalArgumentException("This index supports only one index value.");
    }
    return createIndexValue(indexValues);
  }
  
  protected String createIndexValue(String... indexValues) {
    if(indexValues==null || indexValues.length==0){
      throw new IllegalArgumentException("Please supply at least one index value to use an index.");
    }
    
    if(indexValues.length==1){
      return indexValues[0];
    }
    
    StringBuffer buf=new StringBuffer();
    buf.append(indexValues[0]);
    for(int i=1;i<indexValues.length;i++){
      buf.append("_").append(indexValues[i]);
    }
    return buf.toString();
  }

  
  private synchronized void ensurePrepared(Session s) {
    if(!isInitialized){
      selectStatement = s.prepare(select("val")
          .from(IndexTableHandler.INDEX_TABLE_NAME)
          .where(eq("idx_name", QueryBuilder.bindMarker()))
          .and(eq("idx_value", QueryBuilder.bindMarker())));
      
      insertStatement = s.prepare(insertInto(IndexTableHandler.INDEX_TABLE_NAME)
          .value("idx_name", QueryBuilder.bindMarker())
          .value("idx_value",QueryBuilder.bindMarker())
          .value("val",QueryBuilder.bindMarker()));
      
      deleteStatement = s.prepare(delete().all()
          .from(IndexTableHandler.INDEX_TABLE_NAME)
          .where(eq("idx_name", QueryBuilder.bindMarker()))
          .and(eq("idx_value",QueryBuilder.bindMarker()))
          .and(eq("val",QueryBuilder.bindMarker())));
      
      deleteUniqueStatement = s.prepare(delete().all()
          .from(IndexTableHandler.INDEX_TABLE_NAME)
          .where(eq("idx_name", QueryBuilder.bindMarker()))
          .and(eq("idx_value",QueryBuilder.bindMarker())));
      
      isInitialized=true;
    }
  }
  
}