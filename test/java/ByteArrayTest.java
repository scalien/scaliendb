import com.scalien.scaliendb.*;
import java.util.ArrayList;

public class ByteArrayTest {
   	public static void main(String[] args) {
		try {
            final String[] controllers = {"127.0.0.1:7080"};

			Client client = new Client(controllers);
			client.setTrace(true);

            client.useDatabase("test");
            client.useTable("words");

			// for (int i = 0; i < 10; i++) {
			// 	String value = Integer.toString(i);
			// 	client.setIfNotExists(value, value);
			// }
			
			for (int i = 0; i < 10; i++) {
				String keyString = Integer.toString(i);
				client.get(keyString.getBytes());
				Result result = client.getResult();
				byte[] value = result.getValueData();
				String valueString = new String(value);
				System.out.println(valueString);
			}
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}