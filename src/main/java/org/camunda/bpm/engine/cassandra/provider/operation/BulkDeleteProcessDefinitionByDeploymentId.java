/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.camunda.bpm.engine.cassandra.provider.operation;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;

import java.util.ArrayList;
import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class BulkDeleteProcessDefinitionByDeploymentId implements BulkOperationHandler {

  public void perform(CassandraPersistenceSession session, Object parameter, BatchStatement flush) {
    String deploymentId = (String) parameter;

    Session s = session.getSession();

    List<Row> processDefinitionsToDelete = s.execute(QueryBuilder.select("id", "key", "version").from(ProcessDefinitionTableHandler.TABLE_NAME).where(eq("deployment_id", deploymentId))).all();
    List<String> ids = new ArrayList<String>();

    for (Row processDefinitionToDelete : processDefinitionsToDelete) {
      ids.add(processDefinitionToDelete.getString("id"));

      flush.add(QueryBuilder.delete().all().from(ProcessDefinitionTableHandler.TABLE_NAME_IDX_VERSION)
        .where(eq("key", processDefinitionToDelete.getString("key")))
        .and(eq("version", processDefinitionToDelete.getInt("version"))));
    }

    flush.add(QueryBuilder.delete().all().from(ProcessDefinitionTableHandler.TABLE_NAME).where(in("id", ids)));
  }

}
