package cn.com.tool;

import cn.com.tool.utils.JDBCUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.sql.*;

import java.util.Iterator;

public class ExcelToMysql {

    public static void main(String[] args) throws Exception {
        String excelFilePath = "C:\\Users\\86153\\Desktop\\Tmp\\excel\\y_user.xlsx";
        String mysqlTableName = "y_user";

        Connection conn = JDBCUtil.getConnection();
        if (conn == null)
            return;

        try {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + mysqlTableName + " VALUES (?, ?, ?,?)");

            FileInputStream inputStream = new FileInputStream(excelFilePath);
            //Workbook workbook = WorkbookFactory.create(inputStream);
            Workbook workbook;
            //System.out.println(excelFilePath);
            if (excelFilePath.contains(".xlsx")) {
                workbook = new XSSFWorkbook(OPCPackage.open(inputStream));
            } else {
                workbook = new HSSFWorkbook(inputStream);
            }

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
                conn.commit();
            }

            System.out.println("Data imported successfully to MSQL table " + mysqlTableName);

            //关闭资源
            if (conn != null || stmt != null) {
                conn.close();
                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                conn.rollback();//事务的回滚
                System.out.println("事务的回滚");
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
    }
}