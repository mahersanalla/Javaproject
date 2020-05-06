package olympic;

import olympic.business.Athlete;
import olympic.business.ReturnValue;
import olympic.business.Sport;
import olympic.data.DBConnector;
import olympic.data.PostgreSQLErrorCodes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static olympic.business.ReturnValue.*;

public class Solution {

    private static String ATHLETES_TABLE = "\"Athletes\"";
    private static String COLUMN_ATHLETE_ID = "\"AthleteID\"";
    private static String COLUMN_ATHLETE_NAME = "\"Athletes_Name\"";
    private static String COLUMN_COUNTRY = "\"Country\"";
    private static String COLUMN_ACTIVE = "\"Active\"";


    private static String SPORTS_TABLE = "\"Sports\"";
    private static String COLUMN_SPORT_NAME = "\"SportName\"";
    private static String COLUMN_SPORT_ID = "\"SportID\"";
    private static String COLUMN_CITY = "\"City\"";
    private static String COLUMN_ATHLETES_COUNTER = "\"Athletes_Counter\"";

    private static String PARTICIPATING_TABLE = "\"Participates\"";
    private static String COLUMN_PARTICIPATING_ATHLETE_ID = "\"AthleteID\"";
    private static String COLUMN_PARTICIPATING_SPORT_ID = "\"SportID\"";
    private static String COLUMN_PARTICIPATING_PLACE = "\"Place\"";
    private static String COLUMN_PARTICIPATING_ACTIVE = "\"Active\"";
    private static String COLUMN_PARTICIPATING_PAYMENT = "\"Payment\"";
    private static String COLUMN_PARTICIPATING_COUNTRY = "\"Country\"";
    private static String COLUMN_PARTICIPATING_CITY = "\"City\"";

    private static String FRIENDS_TABLE = "\"Friends\"";
    private static String COLUMN_ATHLETE1 = "\"AthleteID\"";
    private static String COLUMN_ATHLETE2 = "\"FriendID\"";
    private static String SPORTS_VIEW = "\"AthleteSports\"";
    public static void execute_sql_command(String str){
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement(str);
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }
    private static void close_connection(Connection c1,PreparedStatement ps){
        try {
            ps.close();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        try {
            c1.close();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
    }
    //This function creates the views necessary for functions : getCloseAtheltes(), getSportsRecommendations()
    private static void createCloseAthleteViews(Integer athleteId){
        Integer mySportsNum=0;
        Integer common_percentage=0;
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            //The following returns the sports of the given athlete
            String getAthSportsSQL="CREATE VIEW mySports(SportID) AS SELECT "+COLUMN_PARTICIPATING_SPORT_ID+" FROM "+PARTICIPATING_TABLE+
                    " WHERE "+COLUMN_PARTICIPATING_ATHLETE_ID+"="+ athleteId.toString() +";";
            pstmt  = connection.prepareStatement(getAthSportsSQL);
            pstmt.execute();
            //THis SQL COunts the number of sports of the athlete
            String getNumOfSports = "SELECT COUNT(SportID) FROM mySports;";
            pstmt  = connection.prepareStatement(getNumOfSports);
            ResultSet res=pstmt.executeQuery();
            if(res.next()){
                mySportsNum=res.getInt(1);
            }
            if(mySportsNum==0){
                common_percentage=1;
                // If the athlete doesnt participate in any sports,then all others are common with him in  an empty way
            }
            //Find all people who participate in sports
            String getSportMembers = "CREATE VIEW SportMembers(AthleteID,SportID) AS SELECT "+COLUMN_PARTICIPATING_ATHLETE_ID+","
                    +COLUMN_PARTICIPATING_SPORT_ID+" FROM "+ PARTICIPATING_TABLE+" WHERE "+COLUMN_PARTICIPATING_ATHLETE_ID
                    +"<>"+athleteId.toString()+" ;";
            pstmt  = connection.prepareStatement(getSportMembers);
            pstmt.execute();
            //From those we found before, remove tuples that dont contain my sports...
            String getSportsCount = "CREATE VIEW filteredSportsMembers(athleteID,sportID) AS SELECT * FROM SportMembers WHERE " +
                    "SportID IN (SELECT * FROM mySports)";
            pstmt  = connection.prepareStatement(getSportsCount);
            pstmt.execute();
            // Now for each other athlete, count how many common sports he has with the given athlete
            String getCommonSports = "CREATE VIEW commonSportsCount(athleteID,commonSports) AS SELECT athleteID" +
                    ",COUNT(sportID) FROM filteredSportsMembers GROUP BY athleteID ;";
            pstmt  = connection.prepareStatement(getCommonSports);
            pstmt.execute();
            // Calculate the Average for each athlete
            String getCommonPercentage = "CREATE VIEW commonSportsPercentage_A(athleteID,commonPercentage) AS " +
                    "SELECT athleteID,CAST(commonSports AS FLOAT)/CAST("+ mySportsNum.toString() +" AS FLOAT) " +
                    "FROM commonSportsCount";
            pstmt  = connection.prepareStatement(getCommonPercentage);
            pstmt.execute();
            // For athletes who dont participate in any sport, find their percentage as it could or 0 or 1,
            // 0 if i participate in at least one sport then we dont have anything in common,
            // 1 if i dont participate in any sport then, in an empty way we are common...
            String query="CREATE VIEW commonSportsPercentage_B(AthleteId,commonPercentage) AS " +
                    "SELECT "+ COLUMN_ATHLETE_ID +","+ common_percentage.toString() +" FROM "+ ATHLETES_TABLE +
                    " WHERE "+COLUMN_ATHLETE_ID+"<>"+athleteId.toString()+" AND "+COLUMN_ATHLETE_ID+" NOT IN" +
                    " (SELECT athleteID FROM commonSportsPercentage_A);";
            pstmt  = connection.prepareStatement(query);
            pstmt.execute();

            // Union the previous athletes and percentage data
            String query2 = "CREATE VIEW UnionCommonSportsPercentage(athleteID,commonPercentage) AS " +
                    "(SELECT * FROM commonSportsPercentage_A) UNION ALL (SELECT * FROM commonSportsPercentage_B) ;";
            pstmt  = connection.prepareStatement(query2);
            pstmt.execute();

        }
        catch (SQLException e){
            return;
        }
        finally {
            close_connection(connection,pstmt);
        }
    }
    private static void dropCloseAthleteViews() {
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            String query = "DROP VIEW SportMembers CASCADE ;";
            pstmt = connection.prepareStatement(query);
            pstmt.execute();
            String query2 = "DROP VIEW mySports CASCADE ;";
            pstmt = connection.prepareStatement(query2);
            pstmt.execute();
        }
        catch (SQLException e){
        }
        finally {
            close_connection(connection,pstmt);
        }
    }


        public static void createTables() {
        String create_Athletes_command = "CREATE TABLE " + ATHLETES_TABLE + "\n" +
                "(\n" + COLUMN_ATHLETE_ID   +  " INTEGER NOT NULL CHECK (" + COLUMN_ATHLETE_ID +" > 0),\n" +
                COLUMN_ATHLETE_NAME + " TEXT NOT NULL,\n" +
                COLUMN_COUNTRY + " TEXT NOT NULL,\n" +
                COLUMN_ACTIVE + " BOOLEAN NOT NULL  ,\n" +
                "PRIMARY KEY ("+ COLUMN_ATHLETE_ID +"));";

        String create_Sports_command = "CREATE TABLE " + SPORTS_TABLE + "\n" +
                "(\n" + COLUMN_SPORT_ID   +  " INTEGER NOT NULL CHECK (" + COLUMN_SPORT_ID +" > 0),\n" +
                COLUMN_SPORT_NAME + " TEXT NOT NULL,\n" +
                COLUMN_CITY + " TEXT NOT NULL,\n" +
                COLUMN_ATHLETES_COUNTER + " INTEGER default(0) CHECK (" + COLUMN_ATHLETES_COUNTER + ">= 0) ,\n" +
                "PRIMARY KEY ("+ COLUMN_SPORT_ID +"));";

        String create_Participates_command = "CREATE TABLE " + PARTICIPATING_TABLE + "\n" +
                "(\n" + COLUMN_PARTICIPATING_SPORT_ID   +  "INTEGER ,\n" +
                COLUMN_PARTICIPATING_ATHLETE_ID + " INTEGER,\n" +
                COLUMN_PARTICIPATING_PLACE + " INTEGER ,\n" +
                COLUMN_PARTICIPATING_ACTIVE + " BOOLEAN,\n" +
                COLUMN_PARTICIPATING_PAYMENT + "INTEGER default(0) CHECK(" + COLUMN_PARTICIPATING_PAYMENT + ">=0) ,\n"+
                COLUMN_PARTICIPATING_COUNTRY + " TEXT,\n"+
                COLUMN_PARTICIPATING_CITY + " TEXT,\n"+
                "UNIQUE(" + COLUMN_PARTICIPATING_SPORT_ID + ", " + COLUMN_PARTICIPATING_ATHLETE_ID + ")\n," +
                "FOREIGN KEY("+ COLUMN_PARTICIPATING_ATHLETE_ID +")\n" +
                "REFERENCES "+ ATHLETES_TABLE +" (" + COLUMN_ATHLETE_ID + ")\n" +
                "ON DELETE CASCADE,\n" +
                "FOREIGN KEY ("+ COLUMN_PARTICIPATING_SPORT_ID +")\n" +
                "REFERENCES " + SPORTS_TABLE + " (" + COLUMN_SPORT_ID + ")\n" +
                "ON DELETE CASCADE\n" +
                ");";
        String create_Friends_command = "CREATE TABLE " + FRIENDS_TABLE +
                "\n(" + COLUMN_ATHLETE1 + " INTEGER NOT NULL REFERENCES "+ ATHLETES_TABLE + "("+ COLUMN_ATHLETE_ID +") ON DELETE CASCADE"+ ",\n" +
                " " + COLUMN_ATHLETE2 + " INTEGER NOT NULL REFERENCES "+ ATHLETES_TABLE + "("+ COLUMN_ATHLETE_ID +") ON DELETE CASCADE, \n"+
                " PRIMARY KEY("+COLUMN_ATHLETE1 + "," + COLUMN_ATHLETE2 +"));";


        execute_sql_command(create_Athletes_command);
        execute_sql_command(create_Sports_command);
        execute_sql_command(create_Participates_command);
        execute_sql_command(create_Friends_command);


    }

    public static void clearTables() {
        String clr_Athletes_Command = "DELETE FROM " + ATHLETES_TABLE + ";";
        String clr_Sport_Command = "DELETE FROM " + SPORTS_TABLE + ";";
        String clr_Participates_Command = "DELETE FROM " + PARTICIPATING_TABLE + ";";
        String clr_Friends_Command = "DELETE FROM " + FRIENDS_TABLE + ";";

        execute_sql_command(clr_Athletes_Command);
        execute_sql_command(clr_Sport_Command);
        execute_sql_command(clr_Participates_Command);
        execute_sql_command(clr_Friends_Command);

    }

    public static void dropTables() {
        String drop_Athletes_Command = "DROP TABLE " + ATHLETES_TABLE + " CASCADE ;";
        String drop_Sport_Command = "DROP TABLE " + SPORTS_TABLE + " CASCADE ;";
        String drop_Participates_Command = "DROP TABLE " + PARTICIPATING_TABLE + " CASCADE ;";
        String drop_Friends_Command = "DROP TABLE " + FRIENDS_TABLE + " CASCADE ;";

        execute_sql_command(drop_Athletes_Command);
        execute_sql_command(drop_Sport_Command);
        execute_sql_command(drop_Participates_Command);
        execute_sql_command(drop_Friends_Command);
}

    public static ReturnValue addAthlete(Athlete athlete) {
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String query = "INSERT INTO "+ ATHLETES_TABLE + " VALUES(?, ?, ?, ?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, athlete.getId());
            pstmt.setString(2, athlete.getName());
            pstmt.setString(3, athlete.getCountry());
            pstmt.setBoolean(4, athlete.getIsActive());

            pstmt.execute();
        }
        catch (SQLException e){
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()	) {
                return ReturnValue.BAD_PARAMS;
            }
            else if(Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;
            }
            else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static Athlete getAthleteProfile(Integer athleteId) {
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String query = "SELECT * FROM"+ ATHLETES_TABLE + " WHERE " + COLUMN_ATHLETE_ID + " = ?;";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, athleteId);

            ResultSet result = pstmt.executeQuery();
            if (!result.next()) {	// The given sportId does not exist
                return Athlete.badAthlete();
            }
            // Create a new athlete and fill it with the result values.
            Athlete athlete = new Athlete();
            athlete.setId(athleteId);
            athlete.setName(result.getString(2));
            athlete.setCountry(result.getString(3));
            athlete.setIsActive(result.getBoolean(4));
            return athlete;
        }
        catch (SQLException e){
            return Athlete.badAthlete();
        }
        finally {
            close_connection(connection,pstmt);
        }
    }

    public static ReturnValue deleteAthlete(Athlete athlete) {
        /*if (getAthleteProfile(athlete.getId()).getId() == -1) {
            // BadAthlete was returned, so the given Athlete does not exist
            return ReturnValue.NOT_EXISTS;
        }*/
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String query = "DELETE FROM "+ ATHLETES_TABLE + " WHERE " + COLUMN_ATHLETE_ID + " = ?";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, athlete.getId());
            if(pstmt.executeUpdate()==0){
                return NOT_EXISTS;
            }
        }
        catch (SQLException e){
            return ReturnValue.ERROR;
        }
        finally {
            close_connection(connection,pstmt);
        }
        return OK;
    }

    //Haneen
    public static ReturnValue addSport(Sport sport) {
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String query = "INSERT INTO "+ SPORTS_TABLE + " VALUES(?, ?, ?, ?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, sport.getId());
            pstmt.setString(2, sport.getName());
            pstmt.setString(3, sport.getCity());
            pstmt.setInt(4, sport.getAthletesCount());

            pstmt.execute();
        }
        catch (SQLException e){
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()	) {
                return ReturnValue.BAD_PARAMS;
            }
            else if(Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;
            }
            else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            close_connection(connection,pstmt);
        }

        return OK;
    }

    public static Sport getSport(Integer sportId) {
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String query = "SELECT * FROM"+ SPORTS_TABLE + " WHERE " + COLUMN_SPORT_ID + " = ?;";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, sportId);

            ResultSet result = pstmt.executeQuery();
            if (!result.next()) {	// The given sportId does not exist
                return Sport.badSport();
            }

            // Create a new sport and fill it with the result values.
            Sport sport = new Sport();
            sport.setId(sportId);
            sport.setName(result.getString(2));
            sport.setCity(result.getString(3));
            sport.setAthletesCount(result.getInt(4));
            return sport;
        }
        catch (SQLException e){
            return Sport.badSport();
        }
        finally {
            close_connection(connection,pstmt);
        }

    }

    public static ReturnValue deleteSport(Sport sport) {
      /*  if (getSport(sport.getId()).getId() == -1) {
            // BadSport was returned, so the given sport does not exist
            return ReturnValue.NOT_EXISTS;
        }*/
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String query = "DELETE FROM "+ SPORTS_TABLE + " WHERE " + COLUMN_SPORT_ID + " = ?";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, sport.getId());

            if(pstmt.executeUpdate()==0){
                return NOT_EXISTS;
            }
        }
        catch (SQLException e){
            return ReturnValue.ERROR;
        }
        finally {
            close_connection(connection,pstmt);
        }
        return OK;
    }



    public static ReturnValue athleteJoinSport(Integer sportId, Integer athleteId) {
        Athlete athlete = getAthleteProfile(athleteId);
        Sport sport = getSport(sportId);
        if (athlete.getId() == -1 || sport.getId() == -1) {     //Not found
            return ReturnValue.NOT_EXISTS;
        }
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String query = "INSERT INTO "+ PARTICIPATING_TABLE + " VALUES(?, ?, ?, ?, ?,?,?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, sport.getId());     //athleteID
            pstmt.setInt(2, athlete.getId());	//sportID
            pstmt.setInt(3,0);
            Boolean isActive = athlete.getIsActive();
            pstmt.setBoolean(4, isActive); //Active
            if (!isActive) {
                pstmt.setInt(5, 100); //Payment 100$ for observer
            }else {
                pstmt.setInt(5, 0);   //Payment zero for active
            }
            pstmt.setString(6,athlete.getCountry());
            pstmt.setString(7,sport.getCity());
            // place column will be set by default
            pstmt.execute();
/* If the athlete is active and the previous statement was executed successfully,
               we must increase the Athletes counter in the sport*/
            if(isActive) {
                int currentAthletesCounter = sport.getAthletesCount();
                currentAthletesCounter++;
                query = "UPDATE " + SPORTS_TABLE + " SET " + COLUMN_ATHLETES_COUNTER +
                        " = ? WHERE " + COLUMN_SPORT_ID + " = ?;";
                pstmt = connection.prepareStatement(query);
                pstmt.setInt(1, currentAthletesCounter);
                pstmt.setInt(2, sportId);
                pstmt.execute();
            }
        }
        catch (SQLException e){
            if(Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;
            }
            else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            close_connection(connection,pstmt);
        }
        return OK;
    }
    public static ReturnValue athleteLeftSport(Integer sportId, Integer athleteId) {
        Athlete athlete = getAthleteProfile(athleteId);
        Sport sport = getSport(sportId);
        if (athlete.getId() == -1 || sport.getId() == -1) {
            return ReturnValue.NOT_EXISTS;
        }
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();
        try{
            Boolean isActive = athlete.getIsActive();
            String query = "DELETE FROM "+ PARTICIPATING_TABLE + " WHERE (" + COLUMN_PARTICIPATING_SPORT_ID + " = ? AND " +
                    COLUMN_PARTICIPATING_ATHLETE_ID +" = ?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, sportId);	//sportID
            pstmt.setInt(2, athleteId); //athleteID

            if (pstmt.executeUpdate() == 0) {
                return ReturnValue.NOT_EXISTS;
            }

            /* If the athlete is active and the previous statement was executed successfully,
               we must decrease the Athletes counter in the sport*/
            if (isActive) {
                int currentAthletesCounter = sport.getAthletesCount();
                if(currentAthletesCounter<=0) {
                    return OK;
                }
                query = "UPDATE " + SPORTS_TABLE + " SET " + COLUMN_ATHLETES_COUNTER +
                        " = ? WHERE " + COLUMN_SPORT_ID + " = ?;";
                pstmt  = connection.prepareStatement(query);
                pstmt.setInt(1, --currentAthletesCounter);
                pstmt.setInt(2, sportId);
                pstmt.execute();
            }
        }
        catch (SQLException e){
            return ReturnValue.ERROR;
        }
        finally {
            close_connection(connection,pstmt);
        }
        return OK;
    }

    public static ReturnValue confirmStanding(Integer sportId, Integer athleteId, Integer place) {
        Athlete athlete = getAthleteProfile(athleteId);
        Sport sport = getSport(sportId);
        if (athlete.getId() == -1 || sport.getId() == -1) {
            return ReturnValue.NOT_EXISTS;
        }
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();
        try{
            String query0 = "SELECT * FROM " + PARTICIPATING_TABLE +" WHERE (" + COLUMN_PARTICIPATING_SPORT_ID  +
                    " = ? AND " + COLUMN_PARTICIPATING_ATHLETE_ID + " = ?  AND "
                    + COLUMN_PARTICIPATING_ACTIVE+ "=?);";
            pstmt  = connection.prepareStatement(query0);
            pstmt.setInt(1, sportId);   //sportID
            pstmt.setInt(2, athleteId); //athleteID
            pstmt.setBoolean(3, true); // athlete is Active
            ResultSet res=pstmt.executeQuery();
            if(!res.next()){
                return NOT_EXISTS;
            }
            if(place < 1 || place > 3){
                return BAD_PARAMS;
            }
            String query = "UPDATE " + PARTICIPATING_TABLE + " SET " + COLUMN_PARTICIPATING_PLACE +
                    " = ? WHERE (" + COLUMN_PARTICIPATING_SPORT_ID + " = ? AND " + COLUMN_PARTICIPATING_ATHLETE_ID + " = ?  AND "
                    + COLUMN_PARTICIPATING_ACTIVE+ "=?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, place);		//place
            pstmt.setInt(2, sportId);   //sportID
            pstmt.setInt(3, athleteId); //athleteID
            pstmt.setBoolean(4, true); // athlete is Active


            if(pstmt.executeUpdate()==0){
                return NOT_EXISTS;  //No Rows where affected from the update query...
            }
        }
        catch (SQLException e){
            return ReturnValue.ERROR;
        }
        finally {
            close_connection(connection,pstmt);
        }
        return OK;
    }

    public static ReturnValue athleteDisqualified(Integer sportId, Integer athleteId) {
        Athlete athlete = getAthleteProfile(athleteId);
        Sport sport = getSport(sportId);
        if (athlete.getId() == -1 || sport.getId() == -1) {
            return ReturnValue.NOT_EXISTS;
        }
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();
        try{
            String query = "UPDATE " + PARTICIPATING_TABLE + " SET " + COLUMN_PARTICIPATING_PLACE +
                    " = ? WHERE (" + COLUMN_PARTICIPATING_SPORT_ID + " = ? AND " + COLUMN_PARTICIPATING_ATHLETE_ID + " = ?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, 0);		//place
            pstmt.setInt(2, sportId);   //sportID
            pstmt.setInt(3, athleteId); //athleteID
            if(pstmt.executeUpdate()==0){
                return NOT_EXISTS;  //No Rows where affected from the update query...
            }
        }
        catch (SQLException e){
            return ReturnValue.ERROR;
        }
        finally {
            close_connection(connection,pstmt);
        }
        return OK;
    }

    public static ReturnValue makeFriends(Integer athleteId1, Integer athleteId2) {
        Connection connection   = DBConnector.getConnection();
        if(athleteId1.intValue() == athleteId2.intValue()){
            return BAD_PARAMS;
        }
        Athlete a1=getAthleteProfile(athleteId1);
        Athlete a2=getAthleteProfile(athleteId2);
        if(a1.getId()==-1 || a2.getId()==-1){
            return NOT_EXISTS;
        }
        PreparedStatement pstmt = null;
        try{
            String query = "INSERT INTO "+ FRIENDS_TABLE + " VALUES(?, ?),(?,?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, a1.getId());
            pstmt.setInt(2, a2.getId());
            pstmt.setInt(3, a2.getId());
            pstmt.setInt(4, a1.getId());
            pstmt.execute();
        }
        catch (SQLException e){
            if(Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;
            }
            else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static ReturnValue removeFriendship(Integer athleteId1, Integer athleteId2) {
        Connection connection   = DBConnector.getConnection();
        Athlete a1=getAthleteProfile(athleteId1);
        Athlete a2=getAthleteProfile(athleteId2);
        if(a1.getId()==-1 || a2.getId()==-1){
            return NOT_EXISTS;
        }
        PreparedStatement pstmt = null;
        try {
            String query = "DELETE FROM " + FRIENDS_TABLE + " WHERE ((" +
                    COLUMN_ATHLETE1 + "=?" + " AND " + COLUMN_ATHLETE2 + "=?) OR (" +
                    COLUMN_ATHLETE2 + "=?" + " AND " + COLUMN_ATHLETE1 + "=? ));";
            pstmt = connection.prepareStatement(query);
            pstmt.setInt(1, a1.getId());
            pstmt.setInt(2, a2.getId());
            pstmt.setInt(3, a1.getId());
            pstmt.setInt(4, a2.getId());
            if (pstmt.executeUpdate() == 0) {
                return NOT_EXISTS;
            }
        }
        catch (SQLException e){
            return ReturnValue.ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }
    // Haneen - end

    public static ReturnValue changePayment(Integer athleteId, Integer sportId, Integer payment) {
        Athlete athlete = getAthleteProfile(athleteId);
        Sport sport = getSport(sportId);
        if (athlete.getId() == -1 || sport.getId() == -1) {
            return ReturnValue.NOT_EXISTS;
        }
//        if(payment < 0){
//            return BAD_PARAMS;
//        }
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();
        try{
            String query = "UPDATE " + PARTICIPATING_TABLE + " SET " + COLUMN_PARTICIPATING_PAYMENT +
                    " = ? WHERE (" + COLUMN_PARTICIPATING_SPORT_ID + " = ? AND " + COLUMN_PARTICIPATING_ATHLETE_ID + " = ?  AND "
                    + COLUMN_PARTICIPATING_ACTIVE+ "=?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1, payment);		//place
            pstmt.setInt(2, sportId);   //sportID
            pstmt.setInt(3, athleteId); //athleteID
            pstmt.setBoolean(4, false); // athlete is observing only
            if(pstmt.executeUpdate()==0){
                return NOT_EXISTS;  //No Rows where affected from the update query...
            }
        }
        catch (SQLException e){
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            return ReturnValue.ERROR;
        }
        finally {
            close_connection(connection,pstmt);
        }
        return OK;
    }

        public static Boolean isAthletePopular(Integer athleteId) {
        if(getAthleteProfile(athleteId).getId()==-1){
            return false;
        }
        boolean isPopular=false;
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String getAthSportsSQL="CREATE VIEW mySports(SportID) AS SELECT "+COLUMN_PARTICIPATING_SPORT_ID+" FROM "+PARTICIPATING_TABLE+
                    "WHERE "+COLUMN_PARTICIPATING_ATHLETE_ID+"="+ athleteId.toString() +";";

            pstmt  = connection.prepareStatement(getAthSportsSQL);
            pstmt.execute();
            String getFriendsSQL = "CREATE VIEW friendsIDS(friendID) AS SELECT "+ COLUMN_ATHLETE2
                    +"\n FROM "+ FRIENDS_TABLE +" WHERE " + COLUMN_ATHLETE1 + "="+ athleteId.toString() +";";
            pstmt  = connection.prepareStatement(getFriendsSQL);
            pstmt.execute();
            String getFriendsSportsSQL="CREATE VIEW friendsSports AS SELECT DISTINCT "+PARTICIPATING_TABLE +
                    "."+COLUMN_PARTICIPATING_SPORT_ID +" FROM friendsIDS,"
                    + PARTICIPATING_TABLE +" WHERE friendsIDS.friendID="
                    + PARTICIPATING_TABLE + "." +COLUMN_PARTICIPATING_ATHLETE_ID +";";
            pstmt  = connection.prepareStatement(getFriendsSportsSQL);
            pstmt.execute();
            String query="SELECT * FROM friendsSports EXCEPT (SELECT * FROM mySports);";
            pstmt  = connection.prepareStatement(query);
            ResultSet res=pstmt.executeQuery();
            if(res.next()){
                isPopular=false;
            }
            else{
                isPopular=true;
            }
            String query2 = "DROP VIEW mySports CASCADE ;";
            pstmt  = connection.prepareStatement(query2);
            pstmt.execute();
            String query3 = "DROP VIEW friendsIDS CASCADE ;";
            pstmt  = connection.prepareStatement(query3);
            pstmt.execute();
//            String query4 = "DROP VIEW friendsSports CASCADE ;";
//            pstmt  = connection.prepareStatement(query4);
//            pstmt.execute();
            return isPopular;
        }
        catch (SQLException e){
            return false;
        }
        finally {
            close_connection(connection,pstmt);
        }
    }

    public static Integer getTotalNumberOfMedalsFromCountry(String country) {
        int total_medals=0, current_athlete_ID=0;
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();
        try{
            String query ="SELECT COUNT("+COLUMN_PARTICIPATING_ATHLETE_ID +")"+" FROM "+ PARTICIPATING_TABLE +" WHERE ("+ COLUMN_PARTICIPATING_COUNTRY+
                    "=? AND "+ COLUMN_PARTICIPATING_PLACE + ">?);";
                pstmt  = connection.prepareStatement(query);
                pstmt.setString(1,country);
                pstmt.setInt(2,0);
                ResultSet res=pstmt.executeQuery();
                if(res.next()){
                    return res.getInt(1);
                }
                return 0;
        }
        catch (SQLException e){
            return 0;
        }
        finally {
            close_connection(connection,pstmt);
        }
    }

    public static Integer getIncomeFromSport(Integer sportId) {
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();
        try{
            String query = "SELECT SUM(" + COLUMN_PARTICIPATING_PAYMENT + ") FROM "+ PARTICIPATING_TABLE +" WHERE (" + COLUMN_SPORT_ID + " = ?);";
            pstmt  = connection.prepareStatement(query);
            pstmt.setInt(1,sportId);
            ResultSet res=pstmt.executeQuery();
            if(res.next()){
                return res.getInt(1);
            }
            return 0;
        }
        catch (SQLException e){
            return 0;
        }
        finally {
            close_connection(connection,pstmt);
        }
    }

    public static String getBestCountry() {
        ResultSet ordered_countries;
        String bestCountry="";
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();
        try{
            String query = "CREATE VIEW countries_medals(country, medals_count) AS SELECT "+ COLUMN_COUNTRY +
                    " ,COUNT("+ COLUMN_PARTICIPATING_PLACE +") FROM "+ PARTICIPATING_TABLE +
                    " WHERE ("+ COLUMN_PARTICIPATING_PLACE + " > 0)" + " GROUP BY " + COLUMN_PARTICIPATING_COUNTRY + ";";
            pstmt  = connection.prepareStatement(query);
            pstmt.execute();

            String query2 =  "CREATE VIEW ordered_countries(country, medals_count) AS " +
                    "(SELECT * FROM countries_medals ORDER BY medals_count DESC,country ASC); ";
            pstmt  = connection.prepareStatement(query2);
            pstmt.execute();

            String query3 = "SELECT * FROM ordered_countries LIMIT 1";
            pstmt  = connection.prepareStatement(query3);
            ordered_countries = pstmt.executeQuery();
            if (ordered_countries.next()){
                bestCountry = ordered_countries.getString(1);
            }
            String query4 = "DROP VIEW countries_medals CASCADE ;";
            pstmt  = connection.prepareStatement(query4);
            pstmt.execute();
//            String query5 = "DROP VIEW ordered_countries CASCADE ;";
//            pstmt  = connection.prepareStatement(query5);
//            pstmt.execute();
            return bestCountry;
        }
        catch (SQLException e){
            return "";
        }
        finally {
            close_connection(connection,pstmt);
        }
    }

    public static String getMostPopularCity() {
        String popularCity="";
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String getCityAthletesSQL="CREATE VIEW cityAthletes(City,NumOfAthletes) AS SELECT "
                    +COLUMN_PARTICIPATING_CITY+",COUNT("+COLUMN_PARTICIPATING_ATHLETE_ID+
                    ") FROM "+PARTICIPATING_TABLE+ "WHERE "+COLUMN_PARTICIPATING_ACTIVE+"=true "+
                    "\n GROUP BY "+COLUMN_PARTICIPATING_CITY+";";
            pstmt  = connection.prepareStatement(getCityAthletesSQL);
            pstmt.execute();
//21:38
            String getEmptyCitiesSQL="CREATE VIEW emptyCitiesNames(City) AS " +
                    "(SELECT "+COLUMN_CITY +" FROM "+ SPORTS_TABLE +" WHERE "+ COLUMN_CITY +"NOT IN (SELECT City FROM cityAthletes));";// WHERE" + " citySports.City=cityAthletes.City ;";
            pstmt  = connection.prepareStatement(getEmptyCitiesSQL);
            pstmt.execute();

            String addZerosSQL="CREATE VIEW emptyCities(City,NumOfAthletes) AS " +
                    "SELECT City,0 FROM emptyCitiesNames;";
            pstmt  = connection.prepareStatement(addZerosSQL);
            pstmt.execute();

            String unionCitySports="CREATE VIEW unionCities(City,NumOfAthletes) AS SELECT * FROM cityAthletes "+
                    " UNION SELECT * FROM emptyCities";
            pstmt  = connection.prepareStatement(unionCitySports);
            pstmt.execute();

            String getCitySportsSQL="CREATE VIEW citySports(City,NumOfSports) AS SELECT "
                    +COLUMN_PARTICIPATING_CITY+",COUNT("+COLUMN_SPORT_ID+
                    ") FROM "+SPORTS_TABLE+
                    "\n GROUP BY "+COLUMN_CITY+";";
            pstmt  = connection.prepareStatement(getCitySportsSQL);
            pstmt.execute();


            String getCityStatsSQL="CREATE VIEW cityStats AS " +
                    "SELECT * FROM citySports NATURAL JOIN unionCities;";// WHERE" + " citySports.City=cityAthletes.City ;";
            pstmt  = connection.prepareStatement(getCityStatsSQL);
            pstmt.execute();
            String calculateAvgSQL="CREATE VIEW cityAverages(city,AVG) AS " +
                    "SELECT City,CAST(NumOfAthletes AS FLOAT)/CAST(numOfSports AS FLOAT) FROM cityStats;";
            pstmt  = connection.prepareStatement(calculateAvgSQL);
            pstmt.execute();
            String orderCitiesSQL =  "CREATE VIEW ordered_cities AS SELECT *  FROM cityAverages ORDER BY AVG DESC,city DESC ; ";
            pstmt  = connection.prepareStatement(orderCitiesSQL);
            pstmt.execute();
            String getMaxAvgSQL = "SELECT city FROM ordered_cities LIMIT 1 ; ";
            pstmt  = connection.prepareStatement(getMaxAvgSQL);
            ResultSet maxCityAvg=pstmt.executeQuery();
            if (maxCityAvg.next()){
                popularCity = maxCityAvg.getString(1);
            }

            String query2 = "DROP VIEW cityAthletes CASCADE ;";
            pstmt  = connection.prepareStatement(query2);
            pstmt.execute();
            String query3 = "DROP VIEW citySports CASCADE ;";
            pstmt  = connection.prepareStatement(query3);
            pstmt.execute();
            return popularCity;
        }
        catch (SQLException e){
            return "";
        }
        finally {
            close_connection(connection,pstmt);
        }
    }

    public static ArrayList<Integer> getAthleteMedals(Integer athleteId) {
        ArrayList<Integer> arr = new ArrayList<>();
        PreparedStatement pstmt = null;
        Connection c1 = DBConnector.getConnection();
        try{
            String query = "SELECT COUNT("+ COLUMN_PARTICIPATING_ATHLETE_ID + ") FROM "+ PARTICIPATING_TABLE
                    + " WHERE (" + COLUMN_PARTICIPATING_ATHLETE_ID + " = ? AND "+ COLUMN_PARTICIPATING_PLACE + "=?) ;";
            pstmt  = c1.prepareStatement(query);
            pstmt.setInt(1,athleteId);
            //THE FOLLOWING CODE, Could've been done using SQL Only but it would be considered as code replicating,
            // Since i need to run the same command but with different parameters, so we decided to build a small
            // for loop to i run the command with different parameters each time... THANKS :)
            for(int i=0 ; i<3 ; i++) {
                pstmt.setInt(2, i+1);
                ResultSet res = pstmt.executeQuery();
                if (res.next()) {
                    arr.add(res.getInt(1));
                }
            }
        }
        catch (SQLException e){
            return new ArrayList<Integer>();
        }
        finally {
            close_connection(c1,pstmt);
        }
        return arr;
    }

    public static ArrayList<Integer> getMostRatedAthletes() {
        ArrayList<Integer> top10_athletes=new ArrayList<>();
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            String getAthSportsSQL="CREATE VIEW AthletesData(AthleteName,AthleteID,Rank) AS SELECT "+COLUMN_ATHLETE_NAME
                    +","+COLUMN_PARTICIPATING_ATHLETE_ID+","+COLUMN_PARTICIPATING_PLACE+"" +
                    " FROM "+ PARTICIPATING_TABLE+
                    " NATURAL JOIN "+ ATHLETES_TABLE+ " ;";

            pstmt  = connection.prepareStatement(getAthSportsSQL);
            pstmt.execute();

            String query0= "CREATE VIEW AthleteZeroPlace(AthleteName,AthleteID,Points)" +
                    " AS SELECT AthleteName,AthleteID,0  FROM AthletesData WHERE (Rank=0) " +
                    "GROUP BY AthleteID,AthleteName ;";
            pstmt  = connection.prepareStatement(query0);
            pstmt.execute();

            String query = "CREATE VIEW AthleteFirstPlace(AthleteName,AthleteID,FirstPlace)" +
                    " AS SELECT AthleteName,AthleteID,COUNT(*)  FROM AthletesData WHERE (Rank=1) " +
                    "GROUP BY AthleteID,AthleteName ;";
            pstmt  = connection.prepareStatement(query);
            pstmt.execute();
            String query2 = "CREATE VIEW AthleteSecondPlace(AthleteName,AthleteID,SecondPlace)" +
                    " AS SELECT AthleteName,AthleteID,COUNT(*)  FROM AthletesData WHERE (Rank=2) " +
                    "GROUP BY AthleteID,AthleteName ;";
            pstmt  = connection.prepareStatement(query2);
            pstmt.execute();
            String query3= "CREATE VIEW AthleteThirdPlace(AthleteName,AthleteID,Points)" +
                    " AS SELECT AthleteName,AthleteID,COUNT(*)  FROM AthletesData WHERE (Rank=3) " +
                    "GROUP BY AthleteID,AthleteName ;";
            pstmt  = connection.prepareStatement(query3);
            pstmt.execute();
            String query4 = "CREATE VIEW AthleteFirstPoints(AthleteName,AthleteID,Points)" +
                    " AS SELECT AthleteName,AthleteID,FirstPlace*3 FROM AthleteFirstPlace ;";
            pstmt  = connection.prepareStatement(query4);
            pstmt.execute();

            String query5 = "CREATE VIEW AthleteSecondPoints(AthleteName,AthleteID,Points)" +
                    " AS SELECT AthleteName,AthleteID,SecondPlace*2 FROM AthleteSecondPlace ;";
            pstmt  = connection.prepareStatement(query5);
            pstmt.execute();

            String getNotRankedAthletes="CREATE VIEW NotRankedAthletesNames(AthleteName,AthleteId) AS " +
                    "(SELECT "+ COLUMN_ATHLETE_NAME +","+ COLUMN_ATHLETE_ID +" FROM "+ ATHLETES_TABLE +
                    " WHERE "+ COLUMN_ATHLETE_ID +" NOT IN (SELECT AthleteID FROM AthletesData));";// WHERE" + " citySports.City=cityAthletes.City ;";
            pstmt  = connection.prepareStatement(getNotRankedAthletes);
            pstmt.execute();

            String addZerosSQL="CREATE VIEW NotRankedAthletes(AthleteName,AthleteId,Points) AS " +
                    "SELECT AthleteName,AthleteId,0 FROM NotRankedAthletesNames;";
            pstmt  = connection.prepareStatement(addZerosSQL);
            pstmt.execute();

            String query6 = "CREATE VIEW AthleteStats(AthleteName,AthleteID,Points)" +
                    " AS ((SELECT * FROM AthleteZeroPlace) UNION ALL (SELECT * FROM AthleteFirstPoints) " +
                    "UNION ALL (SELECT * FROM AthleteSecondPlace) UNION ALL " +
                    "(SELECT * FROM AthleteThirdPlace) UNION ALL (SELECT * FROM NotRankedAthletes));";
            pstmt  = connection.prepareStatement(query6);
            pstmt.execute();


            String query7=  "CREATE VIEW athleteScore(AthleteName,AthleteId,Points) " +
                    "AS SELECT AthleteName,AthleteID,SUM(points) FROM AthleteStats " + " GROUP BY (AthleteName,AthleteID); ";
            pstmt  = connection.prepareStatement(query7);
            pstmt.execute();

            String query8 =  "CREATE VIEW ordered_athletes(athleteName,athleteId,Points) AS (SELECT * FROM " +
                    "athleteScore ORDER BY Points DESC,AthleteID ASC); ";
            pstmt  = connection.prepareStatement(query8);
            pstmt.execute();

            String query9 = "SELECT * FROM ordered_athletes LIMIT 10";
            pstmt  = connection.prepareStatement(query9);
            ResultSet res = pstmt.executeQuery();
            while(res.next()){
                top10_athletes.add(res.getInt(2));
            }
            String queryDrop = "DROP VIEW AthletesData CASCADE ;";
            pstmt  = connection.prepareStatement(queryDrop);
            pstmt.execute();

            return top10_athletes;
        }
        catch (SQLException e){
            return top10_athletes;
        }
        finally {
            close_connection(connection,pstmt);
        }
    }

    public static ArrayList<Integer> getCloseAthletes(Integer athleteId) {
        ArrayList<Integer> closeAthletes=new ArrayList<Integer>();
        Integer mySportsNum=0;
        Athlete athlete = getAthleteProfile(athleteId);
        if (athlete.getId() == -1) {
            return closeAthletes;
        }
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            createCloseAthleteViews(athleteId);         //Creates the necessary views
            String getCloseAthletes = "SELECT athleteID FROM UnionCommonSportsPercentage " +
                    "WHERE commonPercentage>=0.5 ORDER BY athleteID ASC LIMIT 10";
            pstmt  = connection.prepareStatement(getCloseAthletes);
            ResultSet res2=pstmt.executeQuery();
            while(res2.next()){
                closeAthletes.add(res2.getInt(1));
            }
            dropCloseAthleteViews();
            return closeAthletes;
        }
        catch (SQLException e){
            return closeAthletes;
        }
        finally {
            close_connection(connection,pstmt);
        }
    }
    public static ArrayList<Integer> getSportsRecommendation(Integer athleteId)
    {
        ArrayList<Integer> sportRecommends=new ArrayList<Integer>();
        Integer mySportsNum=0;
        Athlete athlete = getAthleteProfile(athleteId);
        if (athlete.getId() == -1) {
            return sportRecommends;
        }
        Connection connection   = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            createCloseAthleteViews(athleteId);         //Creates the necessary views
            String getCloseAthletes = "CREATE VIEW closeAthletes(athleteID) AS SELECT athleteID FROM UnionCommonSportsPercentage " +
                    "WHERE commonPercentage>=0.5 ORDER BY athleteID ASC";
            pstmt  = connection.prepareStatement(getCloseAthletes);
            pstmt.execute();

            String getCloseAthleteSports = "CREATE VIEW closeSports(athleteID,sportID) AS SELECT * FROM SportMembers " +
                    "WHERE athleteID IN (SELECT * FROM closeAthletes) ;";
            pstmt  = connection.prepareStatement(getCloseAthleteSports);
            pstmt.execute();

            String removeMySportsSQL = "CREATE VIEW closeSportsWithoutMine(sportID) AS SELECT sportID FROM closeSports" +
                    " WHERE sportID NOT IN (SELECT * FROM mySports)";
            pstmt  = connection.prepareStatement(removeMySportsSQL);
            pstmt.execute();

            String getRecommendedSports = "CREATE VIEW temp(sportID,athletesCount) AS SELECT sportID,COUNT(sportID) " +
                    "FROM closeSportsWithoutMine GROUP BY sportID;";
            pstmt  = connection.prepareStatement(getRecommendedSports);
            pstmt.execute();

            String query =  "CREATE VIEW ordered_sports(sportID,athletesCount) AS (SELECT * FROM temp ORDER BY athletesCount DESC,sportID ASC); ";
            pstmt  = connection.prepareStatement(query);
            pstmt.execute();

            String query2 = "SELECT sportID FROM ordered_sports LIMIT 3";
            pstmt  = connection.prepareStatement(query2);
            ResultSet res = pstmt.executeQuery();

            while(res.next()){
                sportRecommends.add(res.getInt(1));
            }

            dropCloseAthleteViews();
            return sportRecommends;
        }
        catch (SQLException e){
            return new ArrayList<Integer>();

        }
        finally {
            close_connection(connection,pstmt);
        }
        //return new ArrayList<Integer>();
    }

}

