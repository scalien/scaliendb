import com.scalien.scaliendb.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Random;

public class ConcurrentWords {
   	public static void main(String[] args) {
		try {
			final String wordsFile = "/usr/share/dict/words";
            final String[] controllers = {"127.0.0.1:7080"};

			System.out.println("Reading words file...");
			FileInputStream fstream = new FileInputStream(wordsFile);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));

			ArrayList<String> words = new ArrayList<String>();
			String line;
 			while ((line = reader.readLine()) != null)
				words.add(line.trim());			
			in.close();
									
			System.out.println("Randomizing words...");
			Random random = new Random(System.nanoTime());
			ArrayList<String> randomWords = new ArrayList<String>();
			for (int i = 0; i < 10000; i++)
			{
				int toRemove = random.nextInt(words.size());
				randomWords.add(words.remove(toRemove));
			}

			System.out.println("Writing words to database...");
			Client client = new Client(controllers);
			//client.setTrace(true);

            client.useDatabase("test");
            client.useTable("words");

			client.begin();
			for (String word : randomWords)
				client.setIfNotExists(word, word);
			client.submit();
			
			System.out.println(randomWords.size() + " words written.");
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}