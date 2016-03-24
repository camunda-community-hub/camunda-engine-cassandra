package org.camunda.bpm.engine.cassandra.provider.query;

import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.cassandra.provider.CassandraPersistenceSession;
import org.camunda.bpm.engine.impl.db.ListQueryParameterObject;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;

public class SelectProcessDefinitionByDeploymentAndKey implements SingleResultQueryHandler<ProcessDefinitionEntity> {

	protected SelectProcessDefinitionsByDeploymentId delegate = new SelectProcessDefinitionsByDeploymentId();
	
	@Override
	@SuppressWarnings("unchecked")
	public ProcessDefinitionEntity executeQuery(
			CassandraPersistenceSession session, Object parameter) {
		
		final Map<String, String> parameterMap = (Map<String, String>) parameter;
		
		final String deploymentId = parameterMap.get("deploymentId");
		final String processDefinitionKey = parameterMap.get("processDefinitionKey");
		
		List<ProcessDefinitionEntity> processDefinitions = delegate.executeQuery(session,
				new ListQueryParameterObject(deploymentId, 0, 100));
		
		for (ProcessDefinitionEntity processDefinitionEntity : processDefinitions) {
			if(processDefinitionKey.equals(processDefinitionEntity.getKey())) {
				return processDefinitionEntity;
			}
		}
		
		return null;
	}

}
