package org.camunda.bpm.engine.cassandra.provider.indexes;

import java.lang.reflect.Field;
import java.util.HashSet;

/**
 * Class to store names of all indexes. Should only be used by the index classes. 
 * Please choose short index names for any new indexes, prefixed with 2 letter entity prefix.
 * Entity in the prefix is the entity being indexed (containing the data in the index value).
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
  
  //variable instance indexes
  static final String PROCESS_ID_BY_VARIABLE_ID = "var_id";

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
