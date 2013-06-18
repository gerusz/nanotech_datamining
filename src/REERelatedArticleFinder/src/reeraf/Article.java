package reeraf;

import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

public class Article {

	public int id;
	public int year;
	public TreeSet<String> keywords;
	public boolean hasReeKeyword;
	public boolean hasChemicalKeyword;
	public TreeSet<String> reeKeywords;
	public TreeSet<String> chemicalKeywords;
	
	public Article() {
		super();
		id = -1;
		keywords = new TreeSet<String>();
		reeKeywords = new TreeSet<String>();
		chemicalKeywords = new TreeSet<String>();
		hasReeKeyword = false;
		hasChemicalKeyword = false;
	}

	public Article(int id) {
		this();
		this.id = id;
	}
	
	public double similarity(Article other) {
		int sameKeywordCount;
		int totalKeywordCount;
		sameKeywordCount = commonKeywords(other);
		totalKeywordCount = keywords.size()+other.keywords.size()-sameKeywordCount;
		return (double)sameKeywordCount/(double)totalKeywordCount;
	}
	
	public int commonKeywords(Article other) {
		int commonCount = 0;
		for(String kw : keywords) {
			if(other.keywords.contains(kw) && !reeKeywords.contains(kw) && !chemicalKeywords.contains(kw)) { //Exclude the REEs; practical
				commonCount++;
			}
		}
		return commonCount;
	}

	public void setKeywords(TreeSet<String> keywords) {
		this.keywords = keywords;
		scanKeywords();
	}
	
	public void scanKeywords() {
		for(String kw : keywords) {
			if(REERelatedArticleFinder.reeKeywords.contains(kw)) {
				hasReeKeyword = true;
				reeKeywords.add(kw);
			}
			if(REERelatedArticleFinder.chemKeywords.contains(kw) && !REERelatedArticleFinder.reeKeywords.contains(kw)) {
				hasChemicalKeyword = true;
				chemicalKeywords.add(kw);
			}
		}
		keywords.removeAll(chemicalKeywords);
		keywords.removeAll(reeKeywords);
	}
	
	public String reeKwString() {
		String output = "";
		if(reeKeywords.size() > 0) {
			Iterator<String> kwIterator = reeKeywords.iterator();
			while(kwIterator.hasNext()) {
				String reeKw = kwIterator.next();
				output += reeKw;
				if(kwIterator.hasNext()) {
					output += ", ";
				}
			}
		}
		return output;
	}
	
	public String chemKwString() {
		String output = "";
		if(chemicalKeywords.size() > 0) {
			Iterator<String> kwIterator = chemicalKeywords.iterator();
			while(kwIterator.hasNext()) {
				String chemKw = kwIterator.next();
				output += chemKw;
				if(kwIterator.hasNext()) {
					output += ", ";
				}
			}
		}
		return output;
	}
	
	public String allKwString() {
		String output = "";
		if(keywords.size() > 0) {
			Iterator<String> kwIterator = keywords.iterator();
			while(kwIterator.hasNext()) {
				String kw = kwIterator.next();
				output += kw;
				if(kwIterator.hasNext()) {
					output += ", ";
				}
			}
		}
		return output;
	}

}
