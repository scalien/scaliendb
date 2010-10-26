import com.scalien.scaliendb.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class MessageBoard
{
    public static void main(String[] args)
	{
        try
		{
            String[] controllers = {"127.0.0.1:7080"};

            // Client.setTrace(true);
            
            Client client = new Client(controllers);

            Database db = new Database(client, "message_board");
            Table table = new Table(client, db, "messages");
			
			long highestSeen = 0;
			while (true)
			{
				String sindex = table.get("index");
				if (sindex != null)
				{
						long index = Long.parseLong(sindex);
						for (long i = highestSeen; i < index; i++)
						{
							String message = table.get(Long.toString(i));
							if (message != null)
							{
								System.out.println();
								System.out.println(i + ". " + message);
								System.out.println();
								System.out.println("-------------");
							}
						}
						highestSeen = index;
				}
				try { Thread.currentThread().sleep(1000); } catch(InterruptedException ie) {}
			}
        }
		catch (SDBPException e)
		{
            System.out.println(e.getMessage());
        }
    }
}