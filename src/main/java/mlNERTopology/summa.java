package mlNERTopology;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.classify.Classification;
import com.aliasi.classify.LogisticRegressionClassifier;
import com.aliasi.crf.ChainCrfChunker;
import com.aliasi.tokenizer.SoundexTokenizerFactory;
import com.aliasi.util.AbstractExternalizable;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;


import java.io.*;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by s1 on 6/11/2017.
 */
public class summa {
    OutputCollector _collector;
    File modelFile ;
    ChainCrfChunker crfChunker;
   // SimpleCrfFeatureExtractor fg;
    public static void main(String[] args) {

        summa su=new summa();
        try {
            su.doSomething();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public void doSomething() throws URISyntaxException, IOException {

        /*//InputStream input = getClass().getResourceAsStream("c3TitleSet1.input");
        System.out.println(getClass().getResource("c3TitleSet1.input").getFile());
       // File modelFile = new File(getClass().getResource("/root/C2CStornCluster/src/main/resources/c3TitleSet1.input").getFile());
        File modelFile = new File("/root/C2CStornCluster/src/main/resources/mlNERTopology/c3TitleSet1.input");


        BufferedReader br = new BufferedReader(new FileReader(modelFile));
        String row;
        int count=0;
        while ((row = br.readLine()) != null) {
            count++;
        }
        System.out.println(count);*/

        /*File modelFile =new File("group_LogReg.model");
        LogisticRegressionClassifier<CharSequence> classifier= null;
        try {
            classifier = (LogisticRegressionClassifier<CharSequence>) AbstractExternalizable.readObject(modelFile);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Classification classification
                = classifier.classify("HP 7 Model 1800 Tablet with Intel Atom Processor 8GB Memory Tablet Case - UniGrip Edition - BLACK (Walmart Exclusive)");
        String group=classification.bestCategory();
        System.out.println(group);*/
        String row="HP 7 Model 1800 Tablet with Intel Atom Processor 8GB Memory Tablet Case - UniGrip Edition - BLACK (Walmart Exclusive";
        Map<String,Set<String>> returnMap= new HashMap<>();
        Set<String>modelSet=new HashSet<>();
        for(String token:row.split(" ")){
            if(isAlphanumeric(token)){
                modelSet.add(token);
            }

        }
        if(modelSet.size()>0){
            returnMap.put("MOD",modelSet);
        }
        System.out.println(returnMap.keySet());



    }
    public boolean isAlphanumeric(String str) {
        boolean isAlpha=false;
        boolean isNumaric=false;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isDigit(c) ) {
                isNumaric = true;
                break;
            }
        }
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isLetter(c)) {
                isAlpha = true;
                break;
            }
        }
        if(isAlpha&&isNumaric){
            return true;
        }

        return false;
    }
}
