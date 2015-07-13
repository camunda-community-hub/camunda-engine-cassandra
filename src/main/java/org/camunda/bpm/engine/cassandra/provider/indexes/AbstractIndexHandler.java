package org.camunda.bpm.engine.cassandra.provider.indexes;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

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

  protected abstract String getTableName();
  protected abstract String getIndexName();
  protected abstract String getIndexValue(T entity);
  protected abstract String getValue(T entity);
  protected abstract boolean isUnique();
  
  @Override
  public String getUniqueValue(CassandraPersistenceSession cassandraPersistenceSession, String ... indexValues){
    if(!isUnique()){
      throw new UnsupportedOperationException("Index "+getIndexName()+" is not unique.");
    }
    
    Session s = cassandraPersistenceSession.getSession();
    List<Row> rows = s.execute(select("val").from(getTableName()).where(eq("idx_name", getIndexName())).and(eq("idx_value", getIndexValue(indexValues)))).all();
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
    List<Row> rows = s.execute(select("val").from(getTableName()).where(eq("idx_name", getIndexName())).and(eq("idx_value", getIndexValue(indexValues)))).all();
    List<String> result = new ArrayList<String>();
    for(Row row:rows){
      result.add(row.getString("val"));
    }
    return result;
  }

  @Override
  public Statement getInsertStatement(T entity) {
    String indexValue = getIndexValue(entity);
    String value = getValue(entity);
    if(indexValue==null || value==null){
      return null;
    }
    
    return QueryBuilder.insertInto(getTableName())
          .value("idx_name", getIndexName())
          .value("idx_value",getIndexValue(entity))
          .value("val",getValue(entity));
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
      buf.append(".").append(indexValues[i]);
    }
    return buf.toString();
  }


  @Override
  public Statement getDeleteStatement(T entity) {
    String indexValue=getIndexValue(entity);
    if(indexValue==null){
      return null;
    }
    if(isUnique()){
      return QueryBuilder.delete().all()
          .from(getTableName())
          .where(eq("idx_name", getIndexName())).and(eq("idx_value",indexValue));
    }
    else{
      String value=getValue(entity);
      if(value==null){
        return null;
      }
      return QueryBuilder.delete().all()
          .from(getTableName())
          .where(eq("idx_name", getIndexName())).and(eq("idx_value",indexValue)).and(eq("val",value));     
    }
  }
  
  public Set<String> crossCheckIndexes(List<Set<String>> sets){
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
}