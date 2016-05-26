package CompYahooOurs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.HashMap;


/**
 * Created by rohit on 5/27/16.
 */
public class Interface {
    public static void main(String args[]) throws Exception {
        File fileName = new File("part-00000");
        HashMap<String, Ads> map = new HashMap<String, Ads>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String temp;
        Ads tempAd;
        while ((temp = br.readLine()) != null) {
            String tempArray[] = temp.split("\t");
            tempAd = new Ads(tempArray[0], tempArray[1], tempArray[3], tempArray[5],
                    Double.parseDouble(tempArray[2]), Double.parseDouble(tempArray[4]), Double.parseDouble(tempArray[6]));

            map.put(temp, tempAd);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String query;
        Ads queriedAd;
        while (true) {
            query = reader.readLine();
            if (map.containsKey(query)) {
                queriedAd = map.get(query);
                System.out.println("Ad 1:" + queriedAd.ad1Name + " Measure : " + queriedAd.ad1Measure);
                System.out.println("Ad 2:" + queriedAd.ad2Name + " Measure : " + queriedAd.ad2Measure);
                System.out.println("Ad 3:" + queriedAd.ad3Name + " Measure : " + queriedAd.ad3Measure);
            } else {
                System.out.println("No ads");
            }
        }
    }

    public static class Ads {
        private String keyphrase;
        private String ad1Name;
        private String ad2Name;
        private String ad3Name;
        private double ad1Measure;
        private double ad2Measure;
        private double ad3Measure;

        public Ads(String keyphrase, String ad1Name, String ad2Name, String ad3Name, double ad1Measure, double ad2Measure, double ad3Measure) {
            this.keyphrase = keyphrase;
            this.ad1Name = ad1Name;
            this.ad2Name = ad2Name;
            this.ad3Name = ad3Name;
            this.ad1Measure = ad1Measure;
            this.ad2Measure = ad2Measure;
            this.ad3Measure = ad3Measure;
        }
    }
}
