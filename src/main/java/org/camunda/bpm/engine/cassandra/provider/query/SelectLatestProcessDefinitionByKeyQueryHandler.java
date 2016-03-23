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
package org.camunda.bpm.engine.cassandra.provider.query;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;
import org.camunda.bpm.engine.impl.db.ListQueryParameterObject;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author Daniel Meyer
 *
 */
public class SelectLatestProcessDefinitionByKeyQueryHandler implements SelectListQueryHandler<ProcessDefinitionEntity, ListQueryParameterObject> {

  public List<ProcessDefinitionEntity> executeQuery(CassandraPersistenceSession session, ListQueryParameterObject parameter) {
    Session s = session.getSession();

    Row row = s.execute(select("id")
      .from(ProcessDefinitionTableHandler.TABLE_NAME_IDX_VERSION)
      .where(eq("key", parameter.getParameter()))
      .limit(1))
      .one();

    if(row == null) {
      return null;
    }

    String id = row.getString("id");

    Row result = s.execute(select()
        .all()
        .from(ProcessDefinitionTableHandler.TABLE_NAME)
        .where(eq("id", id)))
        .one();

    if(result == null) {
      return null;
    }

    CassandraSerializer<ProcessDefinitionEntity> serializer = session.getSerializer(ProcessDefinitionEntity.class);

    return Collections.singletonList(serializer.read(result));

  }

}