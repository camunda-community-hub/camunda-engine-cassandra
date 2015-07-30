package org.camunda.bpm.engine.cassandra.provider.query;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.table.ProcessDefinitionTableHandler;
import org.camunda.bpm.engine.impl.db.ListQueryParameterObject;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;


public class SelectProcessDefinitionByDeploymentId implements SelectListQueryHandler<ProcessDefinitionEntity, ListQueryParameterObject> {
  @Override
  public List<ProcessDefinitionEntity> executeQuery(CassandraPersistenceSession session, ListQueryParameterObject parameter) {
    String deploymentId=(String)parameter.getParameter();
    List<Row> processDefinitionRows = session.getSession().execute(QueryBuilder
        .select("id")
        .from(ProcessDefinitionTableHandler.TABLE_NAME)
        .where(eq("deployment_id", deploymentId))).all();

    List<ProcessDefinitionEntity> result=new ArrayList<ProcessDefinitionEntity>();
    for(Row definitionRow:processDefinitionRows){
      String definitionId=definitionRow.getString("id");
      ProcessDefinitionEntity entity = session.selectById(ProcessDefinitionEntity.class, definitionId);
      if(entity!=null){
        result.add(entity);
      }
    }
    return result;
  }

}
