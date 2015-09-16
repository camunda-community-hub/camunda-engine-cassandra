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
package org.camunda.bpm.engine.cassandra.provider.table;

import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.Session;

/**
 * @author Natalia
 *
 * @date 16/09/2015
 */
public class OrderedIndexTableHandler implements TableHandler {

  public final static String INDEX_TABLE_NAME = "cam_ordered_index";

  protected final String CREATE_INDEX_TABLE = "CREATE TABLE IF NOT EXISTS "+INDEX_TABLE_NAME +" "
      + "(idx_name text, " //name of index
      + "part_id text, " //partition id, ordered indexes have to be partitioned
      + "order_by timestamp, " //index is ordered by this time
      + "val text, " //the object we are indexing
      + "PRIMARY KEY ((idx_name, part_id), order_by, val) ) "
      + "WITH CLUSTERING ORDER BY (order_by ASC);";
  
  protected final static String DROP_INDEX_TABLE = "DROP TABLE IF EXISTS "+INDEX_TABLE_NAME+";";
    
  public List<String> getTableNames() {
    return Collections.singletonList(INDEX_TABLE_NAME);
  }

  public void createTable(Session s) {
    s.execute(CREATE_INDEX_TABLE);
  }

  public void dropTable(Session s) {
    s.execute(DROP_INDEX_TABLE);
  }
  
}
