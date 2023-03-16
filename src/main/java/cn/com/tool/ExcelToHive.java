package cn.com.tool;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public class ExcelToHive {

    public static void main(String[] args) throws Exception {
        String excelFilePath = "path/to/excel/file.xlsx";
        String hiveJdbcUrl = "jdbc:hive2://localhost:10000/default";
        String hiveUsername = "hiveuser";
        String hivePassword = "hivepassword";
        String hiveTableName = "mytable";

        try (Connection conn = DriverManager.getConnection(hiveJdbcUrl, hiveUsername, hivePassword);
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + hiveTableName + " VALUES (?, ?, ?)")) {

            FileInputStream inputStream = new FileInputStream(excelFilePath);
            Workbook workbook = WorkbookFactory.create(inputStream);

            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Iterator<Cell> cellIterator = row.cellIterator();

                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    switch (cell.getCellType()) {
                        case STRING:
                            stmt.setString(cell.getColumnIndex() + 1, cell.getStringCellValue());
                            break;
                        case NUMERIC:
                            stmt.setDouble(cell.getColumnIndex() + 1, cell.getNumericCellValue());
                            break;
                        case BOOLEAN:
                            stmt.setBoolean(cell.getColumnIndex() + 1, cell.getBooleanCellValue());
                            break;
                        default:
                            stmt.setString(cell.getColumnIndex() + 1, "");
                            break;
                    }
                }

                stmt.executeUpdate();
            }

            System.out.println("Data imported successfully to Hive table " + hiveTableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}