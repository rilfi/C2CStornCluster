package mlNERTopology;

import java.io.*;
import java.net.URISyntaxException;

/**
 * Created by s1 on 6/11/2017.
 */
public class summa {
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

        InputStream input = getClass().getResourceAsStream("c3TitleSet1.input");

        BufferedReader br = new BufferedReader(new InputStreamReader(input));
        String row;
        int count=0;
        while ((row = br.readLine()) != null) {
            count++;
        }
        System.out.println(count);
    }
}
