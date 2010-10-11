import com.scalien.scaliendb.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class Test {
        public static void main(String[] args) {
                try {
                        String[] nodes = {"127.0.0.1:7080"};
                        //Client.setTrace(true);
                        Client ks = new Client(nodes);
                        ks.useDatabase("mediafilter");
                        ks.useTable("users");
                        String hol = ks.get("hol");
                        System.out.println(hol);
                        
//                        ArrayList<String> keys = ks.listKeys("", "", 0, false, true);
//                        for (String key : keys)
//                                System.out.println(key);
//                        
//                        Map<String, String> keyvals = ks.listKeyValues("", "", 0, false, true);
//                        for (String key : keyvals.keySet()) {
//                                String value = keyvals.get(key);
//                                System.out.println(key + " => " + value);
//                        }
//         
//                        long cnt = ks.count(new ListParams().setLimit(100));
//                        System.out.println(cnt);
                } catch (Exception e) {
                        System.out.println(e.getMessage());
                }
        }
}