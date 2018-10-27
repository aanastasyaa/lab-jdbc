package laba5;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
* Класс для работы с базой данных HSQLDB с именем groupbd,
* состоящей из таблиц ITEM (книги) и ITEMGROUP (авторы).
* Используется JDBC
*/
public class DBTester {
    private Connection connection=null;
    public DBTester() {}

    private Connection connectToDB() throws ClassNotFoundException, SQLException {
        Connection con = null;
        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        con = DriverManager.getConnection("jdbc:hsqldb:file:bd/groupbd", "SA", "");
        if (con != null) {
            System.out.println("Connection created successfully");

        } else {
            System.out.println("Problem with creating connection");
        }
        return con;
    }

    private void doWork() throws SQLException {
        clearTables();

        createTablesIfNeeded();
        viewGroups();
        viewItems();
        System.out.println("getGroupID(\"Джоан Роулинг\")==1: "+(getGroupID("Джоан Роулинг")==1));
        System.out.println("------Rowling`s books-------");
        viewItemsInGroup("Джоан Роулинг");
        System.out.println("------Add item-------");
        addItemToGroup("После бала", "Лев Толстой");
        viewItems();
        System.out.println("------Remove item-------");
        removeItemFromGroup("Воскресение", "Лев Толстой");
        viewItems();
        System.out.println("------Change items use file-------");
        changeItemsUseFile("items.txt");
        viewItems();
        System.out.println("------Change groups use file-------");
        changeGroupsUseFile("groups.txt");
        viewGroups();
        viewItems();

    }

    private void clearTables() throws SQLException {
        Statement stat=connection.createStatement();
        stat.execute("DROP TABLE ITEM");
        stat.execute("DROP TABLE ITEMGROUP");
        stat.close();
    }
	/**
	* Единственный публичный метод класса,
	* в котором создается подключение к БД, 
	* выполняется метод doWork и закрывается подключение к БД 
	*/
    public void test() {
        try {
            connection = connectToDB();
            doWork();
        }
        catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection!=null && !connection.isClosed()) connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void viewGroups() throws SQLException {
        Statement statement=connection.createStatement();
        ResultSet set=statement.executeQuery("SELECT ID,TITLE FROM ITEMGROUP");
        if(set.isBeforeFirst())
            System.out.println("ID\tTITLE");
        while(set.next())
            System.out.println(set.getString("ID")+"\t"+set.getString("TITLE"));
        System.out.println();
        statement.close();
    }

    private void viewItems() throws SQLException {
        Statement statement=connection.createStatement();
        ResultSet set=statement.executeQuery("SELECT TITLE, GROUPID FROM ITEM");
        if(set.isBeforeFirst())
            System.out.println("TITLE\tGROUPID");
        while(set.next())
            System.out.println(set.getString("TITLE")+"\t"+set.getString("GROUPID"));
        System.out.println();
        statement.close();
    }

    private int getGroupID(String key) throws SQLException {
        PreparedStatement prepStat=connection.prepareStatement("SELECT ID FROM ITEMGROUP WHERE TITLE=?");
        prepStat.setString(1,key);
        try(ResultSet set=prepStat.executeQuery()) {
            int result = -1;
            if (set.next()) {//not empty?
                result = set.getInt("ID");
            }
            prepStat.close();
            return result;
        }
    }

    private void viewItemsInGroup (int groupid) throws SQLException {
        PreparedStatement prepStat=connection.prepareStatement("SELECT TITLE FROM ITEM WHERE GROUPID=?");
        prepStat.setInt(1,groupid);
        ResultSet set=prepStat.executeQuery();
        while(set.next())
            System.out.println(set.getString("TITLE"));
        System.out.println();
        prepStat.close();
    }

    private void viewItemsInGroup (String groupname) throws SQLException {
        PreparedStatement prepStat=connection.prepareStatement(
                "SELECT TITLE FROM ITEM " +
                        "INNER JOIN ITEMGROUP ON ITEM.GROUPID=ITEMGROUP.ID" +
                        " WHERE ITEMGROUP.TITLE=?");
        prepStat.setString(1,groupname);
        ResultSet set=prepStat.executeQuery();
        while(set.next())
            System.out.println(set.getString("TITLE"));
        System.out.println();
        set.close();
        prepStat.close();

    }
	/**
	* Функция создания и начального заполнения таблиц, если они отсутствуют в базе
	*/
    private void createTablesIfNeeded() throws SQLException {
        try(Statement stat = connection.createStatement() ) {
            DatabaseMetaData meta = connection.getMetaData();
            try(
            ResultSet res1 = meta.getTables(null, null, "ITEMGROUP", null);
            ResultSet res2 = meta.getTables(null, null, "ITEM", null)
            ) {
                if (!res1.next() || !res2.next()) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("createTables.sql"), "cp1251"))) {
                        StringBuilder sb = new StringBuilder();
                        String s;
                        while ((s = reader.readLine()) != null) {
                            sb.append(s);
                        }

                        String[] inst = sb.toString().split(";");

                        for (int i = 0; i < inst.length; i++) {
                            if (!inst[i].trim().equals("")) {
                                stat.execute(inst[i]);
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("Reading error .sql");
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private boolean addItemToGroup(String itemName, String groupName) throws SQLException {
        int id=getGroupID(groupName);
        if(id==-1)
            return false;

        try(PreparedStatement
                    prepStat = connection.prepareStatement("INSERT INTO ITEM (TITLE, GROUPID) VALUES (?,?)")) {
            prepStat.setString(1, itemName);
            prepStat.setInt(2, id);
            prepStat.execute();
        }
        catch(SQLException e) {
            return false;
        }

        return true;
    }

    private boolean removeItemFromGroup(String itemName, String groupName) throws SQLException {
        int id=getGroupID(groupName);
        if(id==-1)
            return false;
        PreparedStatement prepStat = null;
        try {
            prepStat = connection.prepareStatement("DELETE FROM ITEM WHERE TITLE=? AND GROUPID=?");
            prepStat.setString(1, itemName);
            prepStat.setInt(2, id);
            prepStat.executeUpdate();
        }
        catch(SQLException e) {
            return false;
        }
        finally {
            if(prepStat!=null)
                prepStat.close();
        }
        return true;
    }
	/**
	* Функция редактирования записей таблицы ITEM с помощью файла,
	* который имеет следующий формат:
	* 	Лев Толстой+Война и мир
	* 	Максим Горький+Детство
	* 	Агата Кристи-Десять негритят
	* Файл обрабатывается отдельной транзакцией – если добавить/удалить не получилось, все операции отменяются 
	*/
    private void changeItemsUseFile(String filename) throws SQLException {
        try(BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream(filename), "cp1251"))) {
            String s;
            connection.setAutoCommit(false);
            boolean done=true;
            while ((s=reader.readLine())!=null) {
                String[] split = s.split("-|\\+");
                if(s.contains("-"))
                    done=removeItemFromGroup(split[1], split[0]);
                else done=addItemToGroup(split[1], split[0]);
                if(!done) {
                    System.out.println("Не получилось добавить/удалить: "+s);
                    connection.rollback();
                    return;
                }
            }
            connection.commit();
        }
        catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }
        catch(Exception e) {
            e.printStackTrace();
            connection.rollback();
            System.out.println("Reading error "+filename);
        }
        finally {
            connection.setAutoCommit(true);
        }
    }
	
	/**
	* Функция редактирования записей таблицы ITEMGROUP с помощью файла,
	* который имеет следующий формат:
	* 	+Лев Толстой
	* 	+Максим Горький
	* 	-Агата Кристи
	* Файл обрабатывается отдельной транзакцией – если добавить/удалить не получилось, все операции отменяются 
	*/
    private void changeGroupsUseFile(String filename) throws SQLException {
        try(BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream(filename), "cp1251"))) {
            String s;
            connection.setAutoCommit(false);
            //нужно запомнить последнюю операцию для каждой группы из файла, именно она определяет действие, которое нужно сделать над группой
            //таким образом совершается только одно действие над одной группой, и нет приоритета операций
            HashMap<String, Character> operationForGroup=new HashMap<String, Character>();
            
            Statement stat=connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE); //изменяемый ResultSet
            while ((s = reader.readLine()) != null) {
                operationForGroup.put(s.substring(1), s.charAt(0));                
            }

            try(ResultSet setGroups=stat.executeQuery("SELECT TITLE FROM ITEMGROUP")) {
                while (setGroups.next()) {
                    String title = setGroups.getString("TITLE");
                    //производим удаление группы, если в Map записан '-'
                    if(operationForGroup.containsKey(title)) {
                        if(operationForGroup.get(title)=='-')
                            setGroups.deleteRow();
                        operationForGroup.remove(title);//не нужно добавлять уже существующую группу в БД
                    }

                }
                //добавляем новые группы, если в Map записан '+'
                for(Map.Entry<String,Character> entry: operationForGroup.entrySet()) {
                    if(entry.getValue()=='+') {
                        setGroups.moveToInsertRow();
                        setGroups.updateString("TITLE", entry.getKey());
                        setGroups.insertRow();
                    }
                }
                
            }
            connection.commit();
            stat.close();
        }
        catch(IOException e) {
            e.printStackTrace();
            System.out.println("Reading error "+filename);
        }
        catch(SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }
        finally {
            connection.setAutoCommit(true);
        }
    }
}
