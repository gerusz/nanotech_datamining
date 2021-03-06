package uk.ac.cam.ch.wwmm.oscar3.recogniser.memm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import opennlp.maxent.Event;
import uk.ac.cam.ch.wwmm.oscar3.Oscar3Props;
import uk.ac.cam.ch.wwmm.ptclib.datastruct.Bag;

/**A simple feature selection for the MEMM.
 * 
 * @author ptc24
 *
 */
final class FeatureSelector {

	Bag<String> featureCounts;
	Bag<String> outcomeCounts;
	Bag<String> outcomeCountsByFeature;
	Map<String,Bag<String>> featureToOutcomes;
	Set<String> bannedClones;
	int totalEvents;
	int totalFeatures;
	double defaultThreshold = 0.25;
	
	public FeatureSelector() {
		
	}

	public List<Event> selectFeatures(List<Event> events) {
		return selectFeatures(events, defaultThreshold);
	}
	
	public List<Event> selectFeatures(List<Event> events, double threshold) {
		
		if(Oscar3Props.getInstance().verbose) System.out.print("Selecting features...\t");
		
		totalEvents = events.size();
		totalFeatures = 0;
		featureCounts = new Bag<String>();
		outcomeCounts = new Bag<String>();
		outcomeCountsByFeature = new Bag<String>();
		featureToOutcomes = new HashMap<String,Bag<String>>();
		
		int eventNo = 0;
		for(Event event : events) {
			String outcome = event.getOutcome();
			outcomeCounts.add(outcome);
			String [] features = event.getContext();
			for(int i=0;i<features.length;i++) {
				String feature = features[i];
				featureCounts.add(feature);
				if(!featureToOutcomes.containsKey(feature)) featureToOutcomes.put(feature, new Bag<String>());
				featureToOutcomes.get(feature).add(outcome);
				outcomeCountsByFeature.add(outcome);
				totalFeatures++;
			}
			eventNo++;
		}
		

		List<String> possibleOutcomes = new ArrayList<String>(outcomeCounts.getSet());
		if(possibleOutcomes.size() < 2) return events;
		long [] observed = new long[possibleOutcomes.size()];
		double [] expected = new double[possibleOutcomes.size()];
		double [] expectedForOne = new double[possibleOutcomes.size()];
		
		for(int i=0;i<possibleOutcomes.size();i++) {
			expectedForOne[i] = outcomeCounts.getCount(possibleOutcomes.get(i)) * 1.0 / totalEvents;
		}
		
		Set<String> allowableFeatures = new HashSet<String>();
		for(String feature : featureCounts.getSet()) {
			if(feature.equals("EMPTY") && feature.equals("prior")) {
				allowableFeatures.add(feature);
				continue;
			}
			if(feature.startsWith("PROTECT:")) {
				allowableFeatures.add(feature);
				continue;				
			}
			int totalObserved = featureCounts.getCount(feature);
			for(int i=0;i<possibleOutcomes.size();i++) {
				expected[i] = expectedForOne[i] * totalObserved;
				observed[i] = featureToOutcomes.get(feature).getCount(possibleOutcomes.get(i));
			}
			
			// http://en.wikipedia.org/wiki/G-test
			double g = 0.0;
			for(String outcome : outcomeCounts.getSet()) {
				int coCount = featureToOutcomes.get(feature).getCount(outcome);
				
				if(coCount == 0) continue;
				int outCount = outcomeCounts.getCount(outcome);
				int featCount = featureCounts.getCount(feature);
				double coProb = coCount / (totalFeatures * 1.0);
				double outProb = outCount / (totalEvents * 1.0);
				outProb = outcomeCountsByFeature.getCount(outcome) / (totalFeatures * 1.0);
				
				double featProb = featCount / (totalFeatures * 1.0);
				g += 2 * coCount * Math.log(coProb / (outProb * featProb));
			}
			if(g > threshold) allowableFeatures.add(feature);
		}
		
		//for(String feature : allowableFeatures) {
		//	System.out.println(feature);
		//}
		
		List<Event> newEvents = new ArrayList<Event>();
		for(Event event : events) {
			List<String> features = new ArrayList<String>();
			for(int i=0;i<event.getContext().length;i++) {
				String feature = event.getContext()[i];
				if(allowableFeatures.contains(feature)) features.add(feature);
			}
			Event newEvent = new Event(event.getOutcome(), features.toArray(new String[0]));
			newEvents.add(newEvent);
		}

		if(Oscar3Props.getInstance().verbose) System.out.println("Selected " + allowableFeatures.size() + " features from " + featureCounts.size());
		return newEvents;
	}
	
}
