package ontology;

public class Relation {
	private String noun;
	private String relation;
	private String object;
	
	public Relation(String noun, String relation, String object) {
		super();
		this.noun = noun;
		this.relation = relation;
		this.object = object;
	}
	public String getNoun() {
		return noun;
	}
	public String getRelation() {
		return relation;
	}
	public String getObject() {
		return object;
	}

}
