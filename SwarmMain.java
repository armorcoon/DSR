import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Scanner;

/*************************************************
 *
 * Description: Main method for triggering the SwarmDataReader
 *
 * (DSR Assignment 2)
 *
 * @author Panupong Promnikorn
 * @date May 2020
 *
 *************************************************/
public class SwarmMain
{
    // Main menu constants
    private final int CONTINUE = 0;
    private final int POPULATE = 1;
    private final int GROUP = 2;
    private final int ORDER = 3;
    private final int EXIT = 4;

    public void runUI(String filename)
    {
        SwarmDataReader sdr = new SwarmDataReader(filename);

        try
        {
            int option = CONTINUE;
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

            while (option != EXIT)
            {
                System.out.println("WELCOME TO THE SWARM DATABASE");
                System.out.println("Please select an option:");
                System.out.println("[1] Transfer from file to DB");
                System.out.println("[2] Get Group Metric");
                System.out.println("[3] Get Order Metric");
                System.out.println("[4] Exit");
                option = Integer.parseInt(in.readLine());
                Scanner s = new Scanner(System.in);

                switch(option)
                {
                    case POPULATE:
                        sdr.transferFileToDB();
                        System.out.println("Data has been transferred.");
                        break;
                    case GROUP:
                        System.out.println("Enter clock tick: ");
                        int groupClockTick = s.nextInt();
                        System.out.println("Group: " + sdr.getGroupMetric(groupClockTick));
                        System.out.println();
                        break;
                    case ORDER:
                        System.out.println("Enter clock tick: ");
                        int orderClockTick = s.nextInt();
                        System.out.println("Order: " + sdr.getOrderMetric(orderClockTick));
                        System.out.println();
                        break;
                    case EXIT:
                        System.out.println("Bye");
                        break;
                    default:
                        System.out.println("Please enter a valid option [1-4].");
                        break;
                }

            }

        }
        catch(IOException | SQLException ioe)
        {
            ioe.printStackTrace();
        }
    }



    /**
     * Run the data reader with one file, and compute group and order metric for that scenario.
     *
     * @param args No args required
     */
    public static void main(String[] args)
    {
        SwarmMain m = new SwarmMain();
        m.runUI("flocking");

    }
}
