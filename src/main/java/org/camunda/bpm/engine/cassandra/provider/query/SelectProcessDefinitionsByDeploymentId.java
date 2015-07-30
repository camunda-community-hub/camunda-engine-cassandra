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

import java.util.ArrayList;
import java.util.List;

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
public class SelectProcessDefinitionsByDeploymentId implements SelectListQueryHandler<ProcessDefinitionEntity, ListQueryParameterObject> {

  public List<ProcessDefinitionEntity> executeQuery(CassandraPersistenceSession session, ListQueryParameterObject parameter) {
    Session s = session.getSession();

    List<Row> rows = s.execute(select().all()
      .from(ProcessDefinitionTableHandler.TABLE_NAME)
      .where(eq("deployment_id", parameter.getParameter())))
      .all();

    CassandraSerializer<ProcessDefinitionEntity> serializer = session.getSerializer(ProcessDefinitionEntity.class);

    List<ProcessDefinitionEntity> resultList = new ArrayList<ProcessDefinitionEntity>();
    for (Row row : rows) {
      resultList.add(serializer.read(row));
    }

    return resultList;
  }

}
