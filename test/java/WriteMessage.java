import com.scalien.scaliendb.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import java.util.Scanner;

public class WriteMessage
{
    public static void main(String[] args)
	{
        try
		{
			Scanner in = new Scanner(System.in);
			
            String[] controllers = {"127.0.0.1:7080"};

            // Client.setTrace(true);
            
            Client client = new Client(controllers);

            Database db = new Database(client, "message_board");
            Table table = new Table(client, db, "messages");
			
			System.out.print("Author: ");
			String author = in.nextLine();
			
			System.out.print("Body: ");
			String body = in.nextLine();
			
			String message = author + ": " + body;
			
			long index = table.add("index", 1);
			
			table.set(Long.toString(index - 1), message);
			
			System.out.println("Message saved.");
        }
		catch (SDBPException e)
		{
            System.out.println(e.getMessage());
        }
    }
}