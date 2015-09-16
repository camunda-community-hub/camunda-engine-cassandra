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

import java.lang.reflect.Field;
import java.util.HashSet;

/**
 * Class to store names of all indexes. Should only be used by the index classes. 
 * Please choose short index names for any new indexes, prefixed with 2 letter entity prefix.
 * 
 * @author Natalia Levine
 *
 * @created 12/07/2015
 */

class IndexNames {
  //event subscription indexes
  static final String EXECUTION_ID_BY_EVENT_NAME = "ev_name";
  static final String PROCESS_ID_BY_EVENT_SUBSCRIPTION_ID = "ev_id";

  //execution indexes
  static final String PROCESS_ID_BY_BUSINESS_KEY = "ex_bkey";
  static final String PROCESS_ID_BY_EXECUTION_ID = "ex_id";
  static final String EXECUTION_ID_BY_PROCESS_ID = "ex_pr_id";
  
  //variable instance indexes
  static final String PROCESS_ID_BY_VARIABLE_ID = "var_id";
  static final String EXECUTION_ID_BY_VARIABLE_VALUE = "var_ex";
  static final String PROCESS_ID_BY_PROCESS_VARIABLE_VALUE = "var_pr";

  //job indexes
  static final String JOBS_BY_CONFIGURATION = "job_cfg";
  static final String EXCLUSIVE_JOBS_BY_DUE_DATE = "job_due";
  static final String EXCLUSIVE_JOBS_BY_LOCK_TIME = "job_lck";
  static final String JOBS_BY_EXECUTION_ID = "job_ex";
  
  //job definition indexes
  static final String JOB_DEF_ID_BY_PROC_DEF_ID = "jd_pr";
  
  static {
    //this is just ensuring uniqueness of index names - might be useful if a lot of new indexes are added
    HashSet <String> set = new HashSet <String>();
    Field[] fields=IndexNames.class.getDeclaredFields();
    try {
      for(Field field:fields){
        String val=(String) field.get(null);
        if(set.contains(val)){
          throw new RuntimeException("Index names are not unique.");
        }
        set.add(val);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialise index names.", e);
    }
  }
}
