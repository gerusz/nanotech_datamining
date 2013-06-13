package ontology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefChain.CorefMention;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefChainAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;

public class RelationExtraction {
	public static List<Relation> extractRelations(String text, String[] keywords) {
		List<Relation> relations = new ArrayList<Relation>();
		Properties props = new Properties();
		props.put("annotators",
				"tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		// create an empty Annotation just with the given text
		Annotation document = new Annotation(text);

		// run all Annotators on this text
		pipeline.annotate(document);

		// these are all the sentences in this document
		// a CoreMap is essentially a Map that uses class objects as keys and
		// has values with custom types
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);

		for (CoreMap sentence : sentences) {
			// traversing the words in the current sentence
			// a CoreLabel is a CoreMap with additional token-specific methods
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				// this is the text of the token
				String word = token.get(TextAnnotation.class);
				// this is the POS tag of the token
				String pos = token.get(PartOfSpeechAnnotation.class);
				// this is the NER label of the token
				String ne = token.get(NamedEntityTagAnnotation.class);
			}

			// this is the parse tree of the current sentence
			Tree tree = sentence.get(TreeAnnotation.class);

			// this is the Stanford dependency graph of the current sentence
			SemanticGraph dependencies = sentence
					.get(CollapsedCCProcessedDependenciesAnnotation.class);
			System.out.println(dependencies);
			for (int i = 0; i < keywords.length; ++i) {
				List<IndexedWord> nodes = dependencies
						.getAllNodesByWordPattern(keywords[i]);
				for (IndexedWord w : nodes) {
					for (int j = 0; j < keywords.length; ++j) {
						System.out.println(keywords[i] + " and " + keywords[j]);
						List<IndexedWord> nnodes = dependencies
								.getAllNodesByWordPattern(keywords[j]);
						for (IndexedWord ww : nnodes) {
							if (!w.equals(ww)) {

							}
						}
					}
				}
			}
		}

		// This is the coreference link graph
		// Each chain stores a set of mentions that link to each other,
		// along with a method for getting the most representative mention
		// Both sentence and token offsets start at 1!
		Map<Integer, CorefChain> graph = document
				.get(CorefChainAnnotation.class);
		System.out.println(graph);
		System.out.println(graph.get(graph.keySet().iterator().next())
				.getRepresentativeMention());
		return null;
	}

	public static void main(String[] args) {
		extractRelations(
				"She was walking lazily, for the fierce April sun was directly overhead. Her umbrella blocked its rays but nothing blocked the heat - the sort of raw, wild heat that crushes you with its energy. A few buffalo were tethered under coconuts, browsing the parched verges. Occasionally a car went past, leaving its treads in the melting pitch like the wake of a ship at sea. Otherwise it was quiet, and she saw no-one.",
				"BEHAVIOR; BLOCK COPOLYMERS; DEPENDENCE; EXTENSIONAL FLOW; IONIC INTERACTION; IONOMER MELTS; MOLECULAR-WEIGHT; POLYMER MELTS; TRIBLOCK COPOLYMER; ELONGATIONAL; ELONGATIONAL"
						.split("; "));
	}
}
