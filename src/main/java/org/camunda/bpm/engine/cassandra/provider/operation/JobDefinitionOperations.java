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
package org.camunda.bpm.engine.cassandra.provider.operation;

import static org.camunda.bpm.engine.cassandra.provider.table.JobDefinitionTableHandler.TABLE_NAME;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.cfg.CassandraProcessEngineConfiguration;
import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.indexes.IndexHandler;
import org.camunda.bpm.engine.cassandra.provider.indexes.JobDefinitionIdByProcessDefinitionIdIndex;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

/**
 * @author Natalia Levine
 *
 * @created 15/09/2015
 */

public class JobDefinitionOperations extends AbstractEntityOperationHandler<JobDefinitionEntity> {
  
  private final static String INSERT = "INSERT into "+TABLE_NAME+" ("
      + "id, "
      + "proc_def_id, "
      + "proc_def_key, "
      + "act_id, "
      + "type, "
      + "config, "
      + "priority, "
      + "suspension_state, "
      + "revision "
      + ") values "
      + "(?, ?, ?, ?, ?, ?, ?, ?, ?);";

  private final static String DELETE = "DELETE FROM "+TABLE_NAME+" WHERE id = ?;";

  private static PreparedStatement insertStatement=null;
  private static PreparedStatement deleteStatement=null;
  
  protected static Map<Class<?>, IndexHandler<JobDefinitionEntity>> indexHandlers = new HashMap<Class<?>, IndexHandler<JobDefinitionEntity>>();

  static {
    indexHandlers.put(JobDefinitionIdByProcessDefinitionIdIndex.class, new JobDefinitionIdByProcessDefinitionIdIndex());
  }

  public JobDefinitionOperations(CassandraPersistenceSession cassandraPersistenceSession) {
  } 

  public static void prepare(CassandraProcessEngineConfiguration config) {
      insertStatement = config.getSession().prepare(INSERT);
      deleteStatement = config.getSession().prepare(DELETE);
  }
  
  public void insert(CassandraPersistenceSession session, JobDefinitionEntity entity) {
    CassandraSerializer<JobDefinitionEntity> serializer = CassandraPersistenceSession.getSerializer(JobDefinitionEntity.class);
   
    // insert deployment
    BoundStatement statement = insertStatement.bind();    
    serializer.write(statement, entity);     
    session.addStatement(statement);
    
    for(IndexHandler<JobDefinitionEntity> index:indexHandlers.values()){
      session.addStatement(index.getInsertStatement(session,entity));    
    }
  }

  public void delete(CassandraPersistenceSession session, JobDefinitionEntity entity) {
    session.addStatement(deleteStatement.bind(entity.getId()));

    for(IndexHandler<JobDefinitionEntity> index:indexHandlers.values()){
      session.addStatement(index.getDeleteStatement(session,entity));    
    }
  }

  public void delete(CassandraPersistenceSession session, JobDefinitionEntity entity, BatchStatement flush) {
    flush.add(deleteStatement.bind(entity.getId()));

    for(IndexHandler<JobDefinitionEntity> index:indexHandlers.values()){
      flush.add(index.getDeleteStatement(session,entity));    
    }
  }

  public void update(CassandraPersistenceSession session, JobDefinitionEntity entity) {
    throw new UnsupportedOperationException();
  }

  protected Class<JobDefinitionEntity> getEntityType() {
    return JobDefinitionEntity.class;
  }

  protected String getTableName() {
    return TABLE_NAME;
  }

  public static IndexHandler<JobDefinitionEntity> getIndexHandler(Class<?> type){
    return indexHandlers.get(type);
  }

}
