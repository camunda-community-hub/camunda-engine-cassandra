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

import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.Session;

/**
 * @author Natalia Levine
 *
 * @created 15/09/2015
 */

public class JobDefinitionTableHandler implements TableHandler {
  
  public final static String TABLE_NAME = "cam_job_def";

  protected final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE_NAME +" "
      + "(id text, "
      + "proc_def_id text, "
      + "proc_def_key text, "
      + "act_id text, "
      + "type text, "
      + "config text, "
      + "priority int, "
      + "suspension_state int, "
      + "revision int, "
      + "PRIMARY KEY (id));";
  
  protected final static String DROP_TABLE = "DROP TABLE IF EXISTS "+TABLE_NAME;
  
  public List<String> getTableNames() {
    return Arrays.asList(TABLE_NAME);
  }

  public void createTable(Session s) {
    s.execute(CREATE_TABLE);
  }
  
  public void dropTable(Session s) {
    s.execute(DROP_TABLE);
  }
}
