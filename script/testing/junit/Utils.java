import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;


/**
 * Created by Guoquan Zhao on 4/30/18.
 */
public class Utils {
    public static void assertResultsSetEqual(ResultSet results, ExpectedResult expectedResult) throws SQLException {
        int rows = expectedResult.getRows();
        int columns = expectedResult.getColumns();

        for (int i = 0; i < rows; i++) {
            assertTrue(results.next());
            for (int j = 0; j < columns; j++) {
                String returnedString = results.getString(j + 1);
                if (returnedString == null) {
                    assertEquals(expectedResult.getItemAtIndex(i, j), "null");
                } else {
                    assertTrue(results.getString(j + 1).equals(expectedResult.getItemAtIndex(i, j)));
                }
            }
        }
        assertFalse(results.next());
    }
}


class ExpectedResult {
    public ExpectedResult(String expectedResult) {
        String[] rows = expectedResult.split("\n");
        List<String[]> results = Stream.of(rows).map(s -> {
            String[] columns = s.split("\\|");
            String[] collect = Stream.of(columns).map(c -> c.trim()).collect(Collectors.toList()).toArray(new String[0]);
            return collect;
        }).collect(Collectors.toList());
        int num_columns = results.get(0).length;
        assertTrue(results.stream().allMatch(strings -> strings.length == num_columns));
        this.rows = results;
    }

    public int getRows() {
        return this.rows.size();
    }

    public int getColumns() {
        return this.rows.get(0).length;
    }

    public String getItemAtIndex(int row, int column) {
        return this.rows.get(row)[column];
    }

    private List<String[]> rows;


}