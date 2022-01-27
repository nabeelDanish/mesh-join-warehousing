package com.dwh.SQL;

import com.dwh.TransactionRecord;

import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class OutputConnection extends SQLConnection {

    private final HashMap<Integer, Integer> monthQuarter;

    public OutputConnection(String url, String user, String password) {
        super(url, user, password);
        monthQuarter = new HashMap<>();
        monthQuarter.put(1, 1);
        monthQuarter.put(2, 1);
        monthQuarter.put(3, 1);
        monthQuarter.put(4, 2);
        monthQuarter.put(5, 2);
        monthQuarter.put(6, 2);
        monthQuarter.put(7, 3);
        monthQuarter.put(8, 3);
        monthQuarter.put(9, 3);
        monthQuarter.put(10, 4);
        monthQuarter.put(11, 4);
        monthQuarter.put(12, 4);
    }

    private String insertCustomer(TransactionRecord record) {
        String custID = null;
        try {
            PreparedStatement statement = con.prepareStatement("INSERT IGNORE INTO dw_project.customer (CUSTOMER_ID, CUSTOMER_NAME) VALUES ( ?, ? )");
            statement.setString(1, record.customerID);
            statement.setString(2, record.customerName);
            boolean executed = statement.execute();
            custID = record.customerID;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return custID;
    }

    private String insertProduct(TransactionRecord record) {
        String productID = null;
        try {
            PreparedStatement statement = con.prepareStatement("INSERT IGNORE INTO dw_project.product (PRODUCT_ID, PRODUCT_NAME) VALUES ( ?, ? );");
            statement.setString(1, record.productID);
            statement.setString(2, record.productName);
            boolean executed = statement.execute();
            productID = record.productID;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return productID;
    }

    private String insertStore(TransactionRecord record) {
        String storeID = null;
        try {
            PreparedStatement statement = con.prepareStatement("INSERT IGNORE INTO dw_project.store (STORE_ID, STORE_NAME) VALUES ( ?, ? )");
            statement.setString(1, record.storeID);
            statement.setString(2, record.storeName);
            boolean executed = statement.execute();
            storeID = record.storeID;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return storeID;
    }

    private String insertSupplier(TransactionRecord record) {
        String supplierID = null;
        try {
            PreparedStatement statement = con.prepareStatement("INSERT IGNORE INTO dw_project.supplier (SUPPLIER_ID, SUPPLIER_NAME) VALUES ( ?, ? )");
            statement.setString(1, record.supplierID);
            statement.setString(2, record.supplierName);
            boolean executed = statement.execute();
            supplierID = record.supplierID;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return supplierID;
    }

    private String insertTime(TransactionRecord record) {
        Date date = null;
        try {
           date = new SimpleDateFormat("yyyy-MM-dd").parse(record.tDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assert date != null;
        String[] str = record.tDate.split("-");
        int day = Integer.parseInt(str[2]);
        int month = Integer.parseInt(str[1]);
        int year = Integer.parseInt(str[0]);

        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);

        try {
            PreparedStatement statement = con.prepareStatement("INSERT IGNORE INTO dw_project.time (TIME_ID, DAY, DAY_OF_WEEK, MONTH, QUARTER, YEAR) VALUES ( ?, ?, ?, ?, ?, ? )");
            statement.setString(1, record.tDate);
            statement.setInt(2, day);
            statement.setInt(3, dayOfWeek);
            statement.setInt(4, month);
            statement.setInt(5, monthQuarter.get(month));
            statement.setInt(6, year);
            boolean executed = statement.execute();
            return record.tDate;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void loadIntoWarehouse(TransactionRecord record) {
        try {
            String customerID = insertCustomer(record);
            String productID = insertProduct(record);
            String storeID = insertStore(record);
            String supplierID = insertSupplier(record);
            String timeID = insertTime(record);

            PreparedStatement statement = con.prepareStatement("INSERT INTO dw_project.transactions (PRODUCT_ID, STORE_ID, CUSTOMER_ID, SUPPLIER_ID, TIME_ID, QUANTITY, TOTAL_SALE) VALUES ( ?, ?, ?, ?, ?, ?, ? )");

            statement.setString(1, productID);
            statement.setString(2, storeID);
            statement.setString(3, customerID);
            statement.setString(4, supplierID);
            statement.setString(5, timeID);
            statement.setInt(6, record.quantity);
            statement.setFloat(7, record.totalSale);

            boolean executed = statement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
