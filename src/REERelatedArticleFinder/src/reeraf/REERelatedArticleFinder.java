package reeraf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public class REERelatedArticleFinder {

	private class SimilarityMeasure {
		public int id1;
		public int id2;
		public int commonKeywords;
		public double simMeasure;

		public SimilarityMeasure() {
			super();
			id1 = -1;
			id2 = -1;
			commonKeywords = 0;
			simMeasure = 0;
		}

		public SimilarityMeasure(int reeArticleId, int chemArticleId) {
			this();
			id1 = reeArticleId;
			id2 = chemArticleId;
		}

		public double calculate() {
			if(id1 != -1 && id2 != -1) {
				Article a1 = REERelatedArticleFinder.relevantArticles.get(id1);
				Article a2 = REERelatedArticleFinder.relevantArticles.get(id2);
				commonKeywords = a1.commonKeywords(a2);
				simMeasure = a1.similarity(a2);
			}
			return simMeasure;
		}
	}

	private class SimilarityCalculatorThread implements Runnable {

		public HashSet<SimilarityMeasure> simMeasures;
		int threadId;

		public SimilarityCalculatorThread() {
			simMeasures = new HashSet<SimilarityMeasure>();
		}

		public SimilarityCalculatorThread(int t) {
			this();
			threadId = t;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			int done = 0;
			int simCount = simMeasures.size();
			double onePercent = simCount / 100.0;
			for(SimilarityMeasure sim : simMeasures) {
				sim.calculate();
				done++;
				if(done % (int)onePercent == 0) {
					System.out.printf("Thread %d done: %.2f%% (%d of %d)\n", threadId, done/onePercent, done, simCount);
				}
			}

			System.out.printf("**** Thread %d finished ****\n", threadId);
		}

	}

	private class Candidate {
		public HashSet<Article> articles;
		String ree;
		String chem;
		String keyword;

		double yearlyAverageRee;
		double yearlyAverageChem;
		double correlation;
		double sigmaChem;
		double sigmaRee;
		double covariance;
		double betaRee;
		double betaChem;
		HashMap<Integer, Double> deltaChem;
		HashMap<Integer, Double> deltaRee;

		HashMap<Integer, Integer> chemicalArticles;
		HashMap<Integer, Integer> reeArticles;

		public Candidate() {
			articles = new HashSet<Article>();
		}

		public void addArticle(Article article) {
			articles.add(article);
		}

		public void calculate() {
			chemicalArticles = new HashMap<Integer, Integer>();
			reeArticles = new HashMap<Integer, Integer>();

			//Count the articles actually containing the ree and the articles containing the chem
			//(an article can be both)

			for(Article article : articles) {
				if(article.chemicalKeywords.contains(chem)) {
					if(!chemicalArticles.containsKey(article.year)) {
						chemicalArticles.put(article.year, 1);
					}
					else {
						chemicalArticles.put(article.year, chemicalArticles.get(article.year)+1);
					}
				}

				if(article.reeKeywords.contains(ree)) {
					if(!reeArticles.containsKey(article.year)) {
						reeArticles.put(article.year, 1);
					}
					else {
						reeArticles.put(article.year, reeArticles.get(article.year)+1);
					}
				}
			}

			//Get the average per year for both
			yearlyAverageRee = 0;
			yearlyAverageChem = 0;

			for(int i=1998; i<2003; i++) {
				if(chemicalArticles.containsKey(i)) {
					yearlyAverageChem += chemicalArticles.get(i) / 5.0;
				}
				if(reeArticles.containsKey(i)) {
					yearlyAverageRee += reeArticles.get(i) / 5.0;
				}
			}

			//Get the differences and the variance
			deltaRee = new HashMap<Integer, Double>();
			deltaChem = new HashMap<Integer, Double>();

			double sigma2Ree = 0;
			double sigma2Chem = 0;

			for(int i=1998; i<2003; i++) {
				if(chemicalArticles.containsKey(i)) {
					deltaChem.put(i, (double)chemicalArticles.get(i) - yearlyAverageChem);
				}
				else {
					deltaChem.put(i, -1*yearlyAverageChem);
				}

				sigma2Chem += (deltaChem.get(i) * deltaChem.get(i)) / 5.0;

				if(reeArticles.containsKey(i)) {
					deltaRee.put(i, (double)reeArticles.get(i) - yearlyAverageRee);
				}
				else {
					deltaRee.put(i, -1*yearlyAverageRee);
				}

				sigma2Ree += (deltaRee.get(i) * deltaRee.get(i)) / 5.0;
			}

			sigmaChem = Math.sqrt(sigma2Chem);
			sigmaRee = Math.sqrt(sigma2Ree);

			covariance = 0;
			double tempCovarianceChem = 0;
			double tempCovarianceRee = 0;
			for(int i=1998; i<2003; i++) {
				covariance += (deltaChem.get(i) * deltaRee.get(i)) / 5.0;
				tempCovarianceChem += ((2000-i) * deltaChem.get(i)) / 5.0;
				tempCovarianceRee += ((2000-i) * deltaRee.get(i)) / 5.0;
			}

			correlation = covariance / (sigmaChem * sigmaRee);
			betaChem = tempCovarianceChem / 2;
			betaRee = tempCovarianceRee / 2;

		}

		public static final String csvHeader = "ree;chem;connection;correlation;covariance;sigma_ree;sigma_chemical;beta_ree;beta_chemical;sample_size;avgRee;avgChem;deltaRee;deltaChem\n";

		public String csvRow() {
			String output = "";
			output += "\""+ree+"\";";
			output += "\""+chem+"\";";
			output += "\""+keyword+"\";";
			output += Double.toString(correlation) + ";";
			output += Double.toString(covariance) + ";";
			output += Double.toString(sigmaRee) + ";";
			output += Double.toString(sigmaChem) + ";";
			output += Double.toString(betaRee) + ";";
			output += Double.toString(betaChem) + ";";
			output += Integer.toString(articles.size()) + ";";
			output += Double.toString(yearlyAverageRee) + ";";
			output += Double.toString(yearlyAverageChem) + ";";
			for(int i=1998; i<2003; i++) {
				output += deltaRee.get(i) + "|";
			}
			output += ";";
			for(int i=1998; i<2003; i++) {
				output += deltaChem.get(i) + "|";
			}
			output += "\n";
			return output;
		}
	}

	private class CandidateCalculatorThread extends Thread {

		Vector<Candidate> candidates;
		int threadId;

		public CandidateCalculatorThread(int threadId) {
			super();
			candidates = new Vector<Candidate>();
			this.threadId = threadId;
		}

		@Override
		public void run() {
			System.out.printf("**** Thread %d started ****\tCandidates: %d\n", threadId, candidates.size());
			int done = 0;
			double onePercent = (int)candidates.size() / 100;
			if(onePercent == 0) onePercent = 1;

			for(Candidate candidate : candidates) {
				candidate.calculate();
				done++;
				if(done % onePercent == 0) {
					System.out.printf("Thread %d: done %.2f%% (%d of %d)\n", threadId, ((double)done / (double)candidates.size()) * 100.0, done, candidates.size() );
				}
			}

			System.out.printf("**** Thread %d done ****\n", threadId);
		}
	}

	public String host;
	public String user;
	public String pass;
	public String dbName;
	public String outputFile;
	public String outputFileCorrelations;
	public static HashSet<String> reeKeywords;
	public static HashMap<String, String> chemicalSymbols;
	public static HashSet<String> chemKeywords;
	public static HashMap<Integer, Article> relevantArticles;
	public static REERelatedArticleFinder mainInstance;

	public REERelatedArticleFinder() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		reeKeywords = new HashSet<String>();
		chemKeywords = new HashSet<String>();
		relevantArticles = new HashMap<Integer, Article>();
		chemicalSymbols = new HashMap<String, String>();
		mainInstance = new REERelatedArticleFinder();
		mainInstance.findArticles(args);
	}


	public void findArticles(String[] args) {
		for(int i=0; i<args.length; i++) {
			if(i==0) {
				host = args[i];
			}
			if(i==1) {
				user = args[i];
			}
			if(i==2) {
				pass = args[i];
			}
			if(i==3) {
				dbName = args[i];
			}
			if(i==4) {
				outputFile = args[i];
			}
			if(i==5) {
				outputFileCorrelations = args[i];
			}
		}

		if(host.isEmpty()) {
			System.out.println("Host:port?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				host = bufferRead.readLine();
			}
			catch(IOException ex) {

			}
		}

		if(user.isEmpty()) {
			System.out.println("Username?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				user = bufferRead.readLine();
			}
			catch(IOException ex) {

			}
		}

		if(pass.isEmpty()) {
			System.out.println("Password?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				pass = bufferRead.readLine();
			}
			catch(IOException ex) {

			}
		}

		if(dbName.isEmpty()) {
			System.out.println("Database name?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				dbName = bufferRead.readLine();
			}
			catch(IOException ex) {

			}
		}

		if(outputFile.isEmpty()) {
			System.out.println("Output file (stats)?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				outputFile = bufferRead.readLine();
			}
			catch(IOException ex) {

			}
		}

		if(outputFile.isEmpty()) {
			System.out.println("Output file (correlations)?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				outputFileCorrelations = bufferRead.readLine();
			}
			catch(IOException ex) {

			}
		}

		try {
			Class.forName("com.mysql.jdbc.Driver");

			Connection connect = DriverManager.getConnection("jdbc:mysql://"+host+"/"+dbName+"?"
					+ "user="+user+"&password="+pass);

			//Get the list of REE keywords

			String reeSelect = "SELECT chemical_sign, element FROM element_letters";

			Statement queryStatement = connect.createStatement();

			ResultSet reeKeywordsRs = queryStatement.executeQuery(reeSelect);
			while(reeKeywordsRs.next()) {
				//reeKeywords.add(reeKeywordsRs.getString(1));
				reeKeywords.add(reeKeywordsRs.getString(2));
				chemicalSymbols.put(reeKeywordsRs.getString(1), reeKeywordsRs.getString(2));
			}

			System.out.printf("%d R.E.E. keywords got\n", reeKeywords.size());

			//Get the list of REE related keywords
			HashSet<String> reeRelatedKeywords = new HashSet<String>();
			ResultSet rrkrs = queryStatement.executeQuery("SELECT DISTINCT keyword FROM ree_paired_keywords_articles");
			while(rrkrs.next()) {
				reeRelatedKeywords.add(rrkrs.getString(1));
			}

			System.out.printf("%d related keywords got\n", reeRelatedKeywords.size());

			//Get the list of chemical keywords

			ResultSet chemKwRs = queryStatement.executeQuery("SELECT * FROM chemical_keywords");
			while(chemKwRs.next()) {
				chemKeywords.add(chemKwRs.getString(1));
			}

			System.out.printf("%d chemical keywords got\n", chemKeywords.size());

			//Assemble a query for every related article
			String relevantArticleQuery = "SELECT article_id, keyword_plus, publication_year FROM ((SELECT * FROM ree_article_keywords) UNION (SELECT * FROM ree_paired_keywords_articles)) as rel_articles INNER JOIN tbl_articles ON rel_articles.article_id = tbl_articles.id";

			System.out.println("Getting relevant articles (query: "+relevantArticleQuery+" )");

			HashSet<Integer> reeArticles = new HashSet<Integer>();
			HashSet<Integer> chemArticles = new HashSet<Integer>();
			ResultSet relevantArticlesRs = queryStatement.executeQuery(relevantArticleQuery);

			while(relevantArticlesRs.next()) {
				Article tmp = new Article(relevantArticlesRs.getInt(1));
				String[] tmpKeywords = relevantArticlesRs.getString(2).split(";");
				for(int i=0; i<tmpKeywords.length; i++) {
					String keyword = tmpKeywords[i].trim();
					if(chemicalSymbols.containsKey(keyword)) {
						tmp.keywords.add(chemicalSymbols.get(keyword));
					}
					else {
						tmp.keywords.add(keyword);
					}
				}
				tmp.year = relevantArticlesRs.getInt(3);
				relevantArticles.put(tmp.id, tmp);
			}

			System.out.printf("%d relevant articles\n", relevantArticles.size());

			for(Article tmp : relevantArticles.values()) {
				tmp.scanKeywords();
				if(tmp.hasChemicalKeyword) {
					chemArticles.add(tmp.id);
				}
				if(tmp.hasReeKeyword) {
					reeArticles.add(tmp.id);
				}
			}

			System.out.printf("%d ree articles, %d chemical articles\n", chemArticles.size(), reeArticles.size());

			HashMap<Integer, HashMap<Integer, SimilarityMeasure>> similarityMatrix = new HashMap<Integer, HashMap<Integer, SimilarityMeasure>>();

			int threadCount = 8; //I guess 8 will be the correct value, though it could be messed with
			int kwThreshold = 2; //Minimal amount of common keywords to even consider the articles similar

			Vector<SimilarityCalculatorThread> calculators = new Vector<SimilarityCalculatorThread>();
			Vector<Thread> threads = new Vector<Thread>();
			for(int i=0; i<threadCount; i++) {
				calculators.add(new SimilarityCalculatorThread(i));
				threads.add(new Thread(calculators.get(i)));
			}
			int toThread = 0;

			for(int reeArticleId : reeArticles) {
				Article reeArticle = relevantArticles.get(reeArticleId);
				HashMap<Integer, SimilarityMeasure> tmpRow = new HashMap<Integer, SimilarityMeasure>();
				for(int chemArticleId : chemArticles) {
					Article chemArticle = relevantArticles.get(chemArticleId);
					if(reeArticleId != chemArticleId && reeArticle.commonKeywords(chemArticle) >= kwThreshold) {
						SimilarityMeasure sm = new SimilarityMeasure(reeArticleId, chemArticleId);
						tmpRow.put(chemArticleId, sm);
						calculators.get(toThread).simMeasures.add(sm);
						toThread++;
						toThread %= threadCount;
					}
				}
				if(tmpRow.size() > 0) similarityMatrix.put(reeArticleId, tmpRow);
			}

			for(Thread t : threads) {
				t.start();
			}

			System.out.println("Threads started");

			boolean finishedRunning = false;
			while(!finishedRunning) {
				int runningCount = 0;
				for(Thread t : threads) {
					if(t.isAlive()) {
						runningCount++;
					}
				}

				if(runningCount == 0) {
					finishedRunning = true;
				}
				else {
					System.out.printf("Running %d of %d threads...\n", runningCount, threadCount);
				}

				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			//Run completed
			System.out.println("Similarity matrix completed. Saving file...");

			//Open the file
			FileWriter fw = new FileWriter(outputFile);
			BufferedWriter writer = new BufferedWriter(fw);


			//Write header rows

			writer.write("ree_article;chem_article;similarity;common_keyword_count;ree_keywords;chem_keywords;ree_article_all_keywords;chem_article_all_keywords\n");

			//Write the actual data
			for(int reeId : similarityMatrix.keySet()) {
				HashMap<Integer, SimilarityMeasure> row = similarityMatrix.get(reeId);
				for(int chemId : row.keySet()) {
					SimilarityMeasure sm = row.get(chemId);
					String rowString = "";
					rowString += Integer.toString(reeId);
					rowString += ";";
					rowString += Integer.toString(chemId);
					rowString += ";";
					rowString += Double.toString(sm.simMeasure);
					rowString += ";";
					rowString += Integer.toString(sm.commonKeywords);
					rowString += ";";
					rowString += "\"" + relevantArticles.get(reeId).reeKwString() + "\";";
					rowString += "\"" + relevantArticles.get(chemId).chemKwString() + "\";";
					rowString += "\"" + relevantArticles.get(reeId).allKwString() + "\";";
					rowString += "\"" + relevantArticles.get(chemId).allKwString() + "\"";
					rowString += "\n";

					writer.write(rowString);
				}
			}

			//Close the file
			writer.close();
			fw.close();

			System.out.println("Done.");

			//Find the articles containing candidates
			
			double similarityThreshold = 0.03;

			HashMap<String, Vector<Integer>> articlesWithCandidates = new HashMap<String, Vector<Integer>>();

			for(int reeId : similarityMatrix.keySet()) {
				HashMap<Integer, SimilarityMeasure> row = similarityMatrix.get(reeId);
				for(int chemId : row.keySet()) {
					if(row.get(chemId).simMeasure >= similarityThreshold) {
						Article reeArticle = relevantArticles.get(reeId);
						for(String ree : reeArticle.reeKeywords) {
							if(!articlesWithCandidates.containsKey(ree)) {
								articlesWithCandidates.put(ree, new Vector<Integer>());
							}
							articlesWithCandidates.get(ree).add(chemId);
						}
					}
				}
			}

			//Fill out the candidate matrix
			HashMap<String, HashMap<String, HashMap<String, Candidate>>> candidateMatrix = new HashMap<String, HashMap<String, HashMap<String, Candidate>>>();
			for(String ree : articlesWithCandidates.keySet()) {
				if(!candidateMatrix.containsKey(ree)) {
					HashMap<String, HashMap<String, Candidate>> tmpPlane = new HashMap<String, HashMap<String, Candidate>>();
					candidateMatrix.put(ree, tmpPlane);
				}
				HashMap<String, HashMap<String, Candidate>> candidateMatrixPlane = candidateMatrix.get(ree);
				for(int candidateId : articlesWithCandidates.get(ree)) {
					Article candidateArticle = relevantArticles.get(candidateId);
					for(String keyword : candidateArticle.keywords) {
						if(!candidateMatrixPlane.containsKey(keyword)) {
							HashMap<String, Candidate> tmpRow = new HashMap<String, Candidate>();
							candidateMatrixPlane.put(keyword, tmpRow);
						}
						HashMap<String, Candidate> candidateMatrixRow = candidateMatrixPlane.get(keyword);
						for(String chemical : candidateArticle.chemicalKeywords) {
							if(!candidateMatrixRow.containsKey(chemical)) {
								Candidate tmpCandidate = new Candidate();
								candidateMatrixRow.put(chemical, tmpCandidate);
								tmpCandidate.chem = chemical;
								tmpCandidate.keyword = keyword;
								tmpCandidate.ree = ree;
							}
							Candidate candidate = candidateMatrixRow.get(chemical);
							candidate.addArticle(candidateArticle);
						}
					}
				}

			}

			//Calculate correlations of candidates
			Vector<CandidateCalculatorThread> candidateCalculators = new Vector<CandidateCalculatorThread>();
			for(int i=0; i<threadCount; i++) {
				candidateCalculators.add(new CandidateCalculatorThread(i));
			}
			toThread = 0;

			for(String ree : candidateMatrix.keySet()) {
				HashMap<String, HashMap<String, Candidate>> plane = candidateMatrix.get(ree);
				for(String kw : plane.keySet()) {
					HashMap<String, Candidate> row = plane.get(kw);
					for(String chem : row.keySet()) {
						Candidate candidate = row.get(chem);
						if(candidate.articles.size() > 1) {
							candidateCalculators.get(toThread).candidates.add(candidate);
							toThread++;
							toThread %= threadCount;
						}
					}
				}
			}

			for(CandidateCalculatorThread thread : candidateCalculators) {
				thread.start();
			}

			System.out.println("Correlation calculator threads started");

			finishedRunning = false;
			while(!finishedRunning) {
				int runningCount = 0;
				for(Thread t : candidateCalculators) {
					if(t.isAlive()) {
						runningCount++;
					}
				}

				if(runningCount == 0) {
					finishedRunning = true;
				}
				else {
					System.out.printf("Running %d of %d threads...\n", runningCount, threadCount);
				}

				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			System.out.println("Writing correlation file");

			fw = new FileWriter(outputFileCorrelations);
			writer = new BufferedWriter(fw);

			writer.write(Candidate.csvHeader);
			for(String ree : candidateMatrix.keySet()) {
				HashMap<String, HashMap<String, Candidate>> plane = candidateMatrix.get(ree);
				for(String kw : plane.keySet()) {
					HashMap<String, Candidate> row = plane.get(kw);
					for(String chem : row.keySet()) {
						Candidate candidate = row.get(chem);
						if(candidate.articles.size() > 1 && !Double.isNaN(candidate.correlation)) {
							writer.write(candidate.csvRow());
						}
					}
				}
			}

			writer.close();
			fw.close();

			System.out.println("Correlations file written.");



		}
		catch(SQLException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
