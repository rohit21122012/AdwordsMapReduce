package TwoLayer.CompYahooOurs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by rohit on 5/19/16.
 */
public class MyReducer extends Reducer<MyMultiPair, Text, Text, Text> {
    private HashMap<String, AdvertiserBundle> advertiserData;
    private ArrayList<AdvertiserMeasure> adMeasure;
    private Text outputVal = new Text();

    public void reduce(MyMultiPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

//        Calculate till day i and then return ranks for i+1 th day
        for (Text val : values) {
            String[] tokens = val.toString().split("\t");
            AdvertiserBundle ab = new AdvertiserBundle();
            if (advertiserData.containsKey(tokens[1]) == false) {

                ab.setLastBid(Double.valueOf(tokens[4]));
                ab.setImpressions(Integer.valueOf(tokens[5]));
                ab.setClicks(Integer.valueOf(tokens[6]));

                advertiserData.put(tokens[1], ab);
            } else {
                ab = advertiserData.get(tokens);
                ab.setLastBid(Double.valueOf(tokens[4]));

                ab.setImpressions(ab.getImpressions() + Integer.valueOf(tokens[5]));
                ab.setClicks(ab.getClicks() + Integer.valueOf(tokens[6]));

                advertiserData.put(tokens[1], ab);
            }
        }

        Iterator it = advertiserData.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, AdvertiserBundle> pair = (Map.Entry<String, AdvertiserBundle>) it.next();
            String k = pair.getKey();
            AdvertiserBundle v = pair.getValue();
            it.remove();
            Double m = (v.getClicks() / v.getImpressions()) * v.getLastBid();
            adMeasure.add(new AdvertiserMeasure(k, m));
        }

        Collections.sort(adMeasure, new Comparator<AdvertiserMeasure>() {
            @Override
            public int compare(AdvertiserMeasure o1, AdvertiserMeasure o2) {
                return o1.getMeasure().compareTo(o2.getMeasure());
            }
        });

        //Sort the array and return the top k elements
        //First Five Elements to printed
        StringBuilder listOfAds = new StringBuilder();
        int numToShow = adMeasure.size();
        int minNumToShow = Math.min(3, numToShow);
        for (int i = 0; i < minNumToShow; i++) {
            listOfAds.append(adMeasure.get(i).getAdvertiser());
            listOfAds.append(String.valueOf(adMeasure.get(i).getMeasure()));
            listOfAds.append(" ");
        }
        outputVal.set(listOfAds.toString());

        context.write(key.getKeyphrase(), outputVal);
    }

    private class AdvertiserBundle {
        private Integer clicks;
        private Integer impressions;
        private Double lastBid;

        public Integer getClicks() {
            return clicks;
        }

        public void setClicks(Integer clicks) {
            this.clicks = clicks;
        }

        public Integer getImpressions() {
            return impressions;
        }

        public void setImpressions(Integer impressions) {
            this.impressions = impressions;
        }

        public Double getLastBid() {
            return lastBid;
        }

        public void setLastBid(Double lastBid) {
            this.lastBid = lastBid;
        }
    }

    private class AdvertiserMeasure {
        private String advertiser;
        private Double measure;

        public AdvertiserMeasure(String k, Double m) {
            advertiser = k;
            measure = m;
        }

        public Double getMeasure() {
            return measure;
        }

        public String getAdvertiser() {
            return advertiser;
        }

        public void setAdvertiser(String advertiser) {
            this.advertiser = advertiser;
        }
    }
}
