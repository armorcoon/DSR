import java.sql.*;
import java.io.*;

/*************************************************
 *
 * Description: SwarmDataReader class contains different methods such as getScenarioID, transferFileToDB, getGroupMetric,
 * ,getOrderMetric and getEuclidDist methods.
 *
 * -- getScenarioID method will return scenarioID in the current input file.
 * -- transferFileTooDB method will transfer all data from the current file and upload to database.
 * -- getGroupMetric method will calculate the number of the group metric.
 * -- getOrderMetric method will calculate the number of the order metric.
 * -- gerEuclidDist method will calculate the Euclid number which is the distance between two points from dimension x and y.
 * (DSR Assignment 2)
 *
 * @author Panupong Promnikorn
 * @date May 2020
 *
 *************************************************/
public class SwarmDataReader {
    //file name
    private String filename_input;
    //scenario ID
    private int scenarioID;
    //instance variable to be stored from database
    private int id;
    //variable to create connection
    private Connection con;

    //variables for calculating in getGroupMetric and getOrderMetric methods
    private double averagePX;
    private double averagePY;
    private double averageVX;
    private double averageVY;
    //arrays to store numbers from Database then will be calculated in getGroupMetric and getOrderMetric
    double[] posx = new double[200];
    double[] posy = new double[200];
    double[] velx = new double[200];
    double[] vely = new double[200];


    /**
     * the instructor that takes the filename in to String and calls getScenario to be stored to scenarioID
     *
     * @param filename is the String name of filename
     */
    SwarmDataReader(String filename) {
        filename_input = filename;
        scenarioID = getScenarioID();
    }

    /**
     * this will construct the driver to the server database
     *
     * @return the connection of driver
     */
    //to create connection with database
    public Connection connecet() {
        try {
            //establish a driver
            Class.forName("com.mysql.jdbc.Driver");
            //connect to database
            return DriverManager.getConnection("jdbc:mysql://seitux2.adfa.unsw.edu.au/z5233293?useSSL=false&rewriteBatchedStatements=true", "z5233293", "mysqlpass");
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * This method will search the file name in database then return scenarioID
     *
     * @return scenarioID
     */
    public int getScenarioID() {

        try {
            //get a connection
            con = connecet();
            //for testing the connection driver
            //System.out.println("connect");
            //send the query code
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM SwarmScenario ");
            //return all information from database eg. ID and description
            while (rs.next()) {
                int number = rs.getInt("ID");
                String description = rs.getString("Description");
                //if file name matches with description in database then return this ID number
                if (filename_input.equals(description)) {
                    //assign this ID number to instance variable
                    id = number;
                }
            }
            //close connection
            rs.close();
            stmt.close();
            con.close();


            //throw exception if has any error
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        //return this instance variable
        return id;
    }

    /**
     * this method will read a text file then transfer all data in text file to database
     */
    public void transferFileToDB() throws SQLException, IOException {
        //get a connection
        con = connecet();
        //using prepare statement in order to avoid SQL injection and speed up to upload database
        PreparedStatement pstat = con.prepareStatement("INSERT INTO SwarmData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        //read all the information in file text
        BufferedReader br = new BufferedReader(new FileReader("E:\\New folder (5)\\src\\" + filename_input + ".txt"));
        String line = br.readLine();
        //this variable will count how many batch times
        int i=0;
        try {
            while (line != null) {
                //split the line in file text for each data
                String[] line_split = line.split(",|\\(|\\)");
                //set the designated parameters in the sequence of data by splitting data in each line
                pstat.setInt(1, Integer.parseInt(line_split[0]));
                pstat.setInt(2, Integer.parseInt(line_split[1]));
                pstat.setFloat(3, Float.parseFloat(line_split[2]));
                pstat.setFloat(4, Float.parseFloat(line_split[3]));
                pstat.setFloat(5, Float.parseFloat(line_split[4]));
                pstat.setFloat(6, Float.parseFloat(line_split[5]));
                pstat.setFloat(7, Float.parseFloat(line_split[6]));
                pstat.setFloat(8, Float.parseFloat(line_split[7]));
                pstat.setFloat(9, Float.parseFloat(line_split[8]));
                pstat.setFloat(10, Float.parseFloat(line_split[9]));
                pstat.setFloat(11, Float.parseFloat(line_split[10]));
                pstat.setFloat(12, Float.parseFloat(line_split[11]));
                pstat.setInt(13, Integer.parseInt(line_split[12]));
                pstat.setInt(14, Integer.parseInt(line_split[13]));
                pstat.setInt(15, scenarioID);
                //add a set of parameters to the PreparedStatement
                pstat.addBatch();
                //every 10,000 times of batch will be executed
                if(i%10000 ==0){
                    System.out.println("execute batch");
                    pstat.executeBatch();
                    pstat.clearBatch();
                }
                i++;
                //read next line
                line = br.readLine();

            }
            pstat.executeBatch();
        } catch (IOException e) {
            e.printStackTrace();
            //close all the connection
        } finally {
            try {
                pstat.close();
                con.close();
                br.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * This method will calculate average distance of all boids from the center of mass of all the boids at the given time(clocktick)
     *
     * @param clocktick is given time
     * @return average distance of all boids from the center of mass of boids
     */

    public double getGroupMetric(int clocktick) {
        //instance variables
        double temp = 0;
        double tempx = 0;
        double tempy = 0;

        try {
            //get a connection
            con = connecet();
            //sending SQL Query statement
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT posx,posy FROM SwarmData WHERE clocktick = '" + clocktick + "'");
            //store posx and posy from each boid in database to posx and posy arrays
            //instance variable to running the array position
            int i = 0;
            while (rs.next()) {

                posx[i] = rs.getDouble("posx");
                posy[i] = rs.getDouble("posy");
                i++;
            }
            //close the connection
            rs.close();
            stmt.close();
            con.close();
            //exception will be throw if any error occurs
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        /**
         * this section for testing only
         * it will show all of number stored in posx and posy arrays
         *
         */
        //  for(int i=0;i<posx.length;i++){
        //  System.out.println(posx[i]);
        //  }
        //  for(int i=0;i<posx.length;i++){
        //      System.out.println(posy[i]);
        //  }

        /**
         * calculate average posx number
         */
        for (int i = 0; i < posx.length; i++) {
            tempx = tempx + posx[i];
        }
        averagePX = tempx / 200;
        /**
         * calculate average posy number
         */
        for (int i = 0; i < posy.length; i++) {
            tempy = tempy + posy[i];
        }
        averagePY = tempy / 200;

        /**
         * calculate group metric number by using getEuclidDist to calculate distance between 2 points
         */
        for (int i = 0; i < posx.length; i++) {
            temp = temp + getEuclidDist(posx[i], posy[i], averagePX, averagePY);
        }
        //group metric number
        return temp / 200;

    }

    /**
     * This method will calculate average distance of all boids' velocity from the center of mass of all the boids at the given time(clocktick)
     *
     * @param clocktick is given time
     * @return average distance of all boids' velocity from the center of mass of boids
     */

    public double getOrderMetric(int clocktick) {
        //instance variables
        double temp = 0;
        double tempvx = 0;
        double tempvy = 0;

        try {
            //get a connection from database
            con = connecet();
            //sending SQL Query statement
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT velx,vely FROM SwarmData WHERE clocktick = '" + clocktick + "'");
            //store velx and vely from each boid in database to velx and vely arrays
            int i = 0;
            //instance variable to running array position
            while (rs.next()) {
                velx[i] = rs.getDouble("velx");
                vely[i] = rs.getDouble("vely");
                i++;
            }

            //close the connection
            rs.close();
            stmt.close();
            con.close();
            //exception will be throw if any error occurs
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        /**
         * this section for testing only
         * it will show all of number stored in velx and vely arrays
         *
         */
        // for (int i = 0; i < velx.length; i++) {
        //     System.out.println(velx[i]);
        // }
        // for (int i = 0; i < vely.length; i++) {
        //     System.out.println(vely[i]);
        // }

        /**
         * calculate velx number
         */
        for (int i = 0; i < velx.length; i++) {
            tempvx = tempvx + velx[i];
        }
        averageVX = tempvx / 200;
        /**
         * calculate vely number
         */
        for (int i = 0; i < vely.length; i++) {
            tempvy = tempvy + vely[i];
        }
        averageVY = tempvy / 200;

        /**
         * calculate group metric number
         */
        for (int i = 0; i < velx.length; i++) {
            temp = temp + getEuclidDist(velx[i], vely[i], averageVX, averageVY);
        }
        //group metric number
        return temp / 200;

    }

    /**
     * this method will calculate EuclidDist number.
     * This value is the distance between 2 points.
     *
     * @param x1 is the number of x dimension from boid
     * @param y1 is the number of y dimension from boid
     * @param x2 is the average of x dimension that calculate from 200 boids
     * @param y2 is the average of y dimension that calculate from 200 boids
     * @return calculated EuclidDist number
     */
    public double getEuclidDist(double x1, double y1, double x2, double y2) {
        return Math.sqrt((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1));
    }


    public static void main(String[] args) throws SQLException, IOException {

        SwarmDataReader test = new SwarmDataReader("random7");
        //test get scenarioID from constructor
        System.out.println(test.scenarioID);
        //test get scenarioID method
        test.getScenarioID();
        //test transferFileToDB method
        test.transferFileToDB();
        //test getGroupMetric and getOrderMetric methods
        // System.out.println(test.getGroupMetric(38003));
        //System.out.println(test.getOrderMetric(38003));

    }
}

