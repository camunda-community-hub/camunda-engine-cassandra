package org.camunda.bpm.engine.cassandra.provider.query;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.ArrayList;
import java.util.List;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.cassandra.provider.serializer.CassandraSerializer;
import org.camunda.bpm.engine.cassandra.provider.table.ResourceTableHandler;
import org.camunda.bpm.engine.impl.db.ListQueryParameterObject;
import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SelectResourcesByDeploymentId implements SelectListQueryHandler<ResourceEntity, ListQueryParameterObject> {

	  public List<ResourceEntity> executeQuery(CassandraPersistenceSession session, ListQueryParameterObject parameter) {
	    Session s = session.getSession();

	    List<Row> rows = s.execute(select().all()
	      .from(ResourceTableHandler.TABLE_NAME)
	      .where(eq("deployment_id", parameter.getParameter())))
	      .all();

	    CassandraSerializer<ResourceEntity> serializer = session.getSerializer(ResourceEntity.class);

	    List<ResourceEntity> resultList = new ArrayList<ResourceEntity>();
	    for (Row row : rows) {
	      resultList.add(serializer.read(row));
	    }

	    return resultList;
	  }

	}