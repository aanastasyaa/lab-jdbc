package laba5.test;

import laba5.DBTester;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

public class TestJDBC {
    private DBTester dbTester;

    @Before
    public void before() {
        dbTester=new DBTester();
        dbTester.test();
    }

    @Test
    public void test() throws SQLException {

    }
}
