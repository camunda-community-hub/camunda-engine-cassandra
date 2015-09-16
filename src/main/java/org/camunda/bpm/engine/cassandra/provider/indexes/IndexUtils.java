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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Natalia
 *
 * @created 16/09/2015
 */
public class IndexUtils {
  
  public static long calculateShard(long date, long shardSizeMillis){
    double shardSequenceNumber=Math.floor(date/shardSizeMillis);
    long shardId=(long) (shardSequenceNumber*shardSizeMillis);
    return shardId;
  }
  
  public static String createIndexValue(String... indexValues) {
    if(indexValues==null || indexValues.length==0){
      throw new IllegalArgumentException("Please supply at least one index value to use an index.");
    }
    
    if(indexValues.length==1){
      return indexValues[0];
    }
    
    StringBuffer buf=new StringBuffer();
    buf.append(indexValues[0]);
    for(int i=1;i<indexValues.length;i++){
      buf.append("_").append(indexValues[i]);
    }
    return buf.toString();
  }
  
  public static Set<String> crossCheckIndexes(Set<String> set1, Set<String> set2){
    List<Set<String>> list=new ArrayList<Set<String>>(2);
    list.add(set1);
    list.add(set2);
    return crossCheckIndexes(list);
  }
  
  public static Set<String> crossCheckIndexes(List<Set<String>> sets){
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
