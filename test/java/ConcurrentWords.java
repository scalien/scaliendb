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

			if (false) {
			    long[] nodes = {100};
			    long quorumID = client.createQuorum(nodes);
			    long databaseID = client.createDatabase("test");
			    long tableID = client.createTable(databaseID, quorumID, "words");
			}

			client.useDatabase("test");
			client.useTable("words");

			client.begin();
			for (String word : randomWords)
				client.setIfNotExists(word, word);
			
			System.out.println("Submitting to database...");
			long startTime = System.currentTimeMillis();
			client.submit();
			long endTime = System.currentTimeMillis();
			
			long wps = (long)(randomWords.size() / (float)(endTime - startTime) * 1000.0 + 0.5);
			System.out.println(randomWords.size() + " words written at " + wps + " word/s");
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}