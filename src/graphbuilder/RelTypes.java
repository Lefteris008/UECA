package graphbuilder;

import org.neo4j.graphdb.RelationshipType;

public enum RelTypes implements RelationshipType {

	MENTION, REPLY, RETWEET, USER, CLUSTER, HUB, OUTLIER, IS_PARENT_OF, CONTRIBUTES_TO, LAST_ID, BELONGS_IN, 
	//this is not needed here (START)
	LAST_TIMESTAMP
	//this is not needed here (STOP)
	
}
