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

import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.DbEntity;

import com.datastax.driver.core.Statement;

/**
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */

public interface IndexHandler <T extends DbEntity>{
  public String getUniqueValue(Map<String, Object> params, CassandraPersistenceSession cassandraPersistenceSession, String ... indexValue);
  public List<String> getValues(Map<String, Object> params, CassandraPersistenceSession cassandraPersistenceSession, String ... indexValue);
  
  public Statement getInsertStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity);
  public Statement getDeleteStatement(CassandraPersistenceSession cassandraPersistenceSession, T entity);
  public List<Statement> getUpdateStatements(CassandraPersistenceSession cassandraPersistenceSession, T entity, T oldEntity);
 
  // public boolean checkIndexMatch(T entity, String ... indexValues);
  public boolean checkIndexMatch(T entity, T newEntity);
  public boolean isUnique();
}
