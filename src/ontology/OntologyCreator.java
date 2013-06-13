package ontology;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.hp.hpl.jena.ontology.OntDocumentManager;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import db.Database;

public class OntologyCreator {
	public static void main(String[] args) throws SQLException {
		Connection connection = Database.getConnection("localhost", "root", "");
		Statement statement = Database.createStatement(connection);
		statement.execute("SELECT abstract, keyword_plus FROM tbl_articles");
		ResultSet resultSet = statement.getResultSet();
		
		OntDocumentManager mgr = new OntDocumentManager();
		OntModelSpec s = new OntModelSpec( OntModelSpec.RDFS_MEM );
		s.setDocumentManager( mgr );
		OntModel m = ModelFactory.createOntologyModel( s );
		
		
	}
}
