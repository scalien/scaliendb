import com.scalien.scaliendb.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class Setup
{
    public static void main(String[] args)
	{
        try
		{
            String[] controllers = {"127.0.0.1:7080"};

            // Client.setTrace(true);
            
            Client client = new Client(controllers);

            long[] nodes = {100};
			long quorumID = client.createQuorum(nodes);
			System.out.println("Creating quorum(" + quorumID + ")...");
			
			long databaseID = client.createDatabase("message_board");
			System.out.println("Creating database(" + databaseID + ")...");

			long tableID = client.createTable(databaseID, quorumID, "messages");
			System.out.println("Create table messages(" + tableID + ")...");

            Database db = new Database(client, "message_board");
            Table table = new Table(client, db, "messages");

			System.out.println("Waiting for primary lease...");
            
            table.set("index", "0");
			System.out.println("Initializing messages:index to 0...");
        }
		catch (SDBPException e)
		{
            System.out.println(e.getMessage());
        }
    }
}