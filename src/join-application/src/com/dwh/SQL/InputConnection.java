package com.dwh.SQL;

import com.dwh.TransactionRecord;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class InputConnection extends SQLConnection {

    public InputConnection(String url, String user, String password) {
        super(url, user, password);
    }

    public int getTransactionsCount() {
        try {
            int counts = -1;
            Statement stnmt = con.createStatement();
            ResultSet rs = stnmt.executeQuery("SELECT COUNT(*) FROM dwhproject.transactions;");
            while (rs.next()) {
                counts = rs.getInt(1);
            }
            return counts;
        } catch (Exception e) {
            return -1;
        }
    }

    public HashMap<String, TransactionRecord> retrieveMasterDataBlock(int offset, int limit) {
        HashMap<String, TransactionRecord> hashMap = new HashMap<>();
        try {
            Statement statement = con.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM dwhproject.masterdata LIMIT " + limit + " OFFSET " + offset + ";");
            while (rs.next()) {
                TransactionRecord transactionRecord = new TransactionRecord();
                transactionRecord.productID = rs.getString(1);
                transactionRecord.productName = rs.getString(2);
                transactionRecord.supplierID = rs.getString(3);
                transactionRecord.supplierName = rs.getString(4);
                transactionRecord.price = rs.getFloat(5);
                hashMap.put(transactionRecord.productID, transactionRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hashMap;
    }

    public ArrayList<TransactionRecord> retrieveRecord(int offset, int limit) {
        ArrayList<TransactionRecord> recordsList = new ArrayList<>();
        try {
            Statement stnmt = con.createStatement();
            ResultSet rs = stnmt.executeQuery("SELECT * FROM dwhproject.transactions LIMIT " + limit + " OFFSET " + offset + ";");
            while (rs.next()) {
                TransactionRecord transaction = new TransactionRecord();

                transaction._id = rs.getInt(1);
                transaction.productID = rs.getString(2);
                transaction.customerID = rs.getString(3);
                transaction.customerName = rs.getString(4);
                transaction.storeID = rs.getString(5);
                transaction.storeName = rs.getString(6);
                transaction.tDate = rs.getString(7);
                transaction.quantity = rs.getInt(8);

                recordsList.add(transaction);
            }
            return recordsList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public int getMDCount() {
        try {
            int counts = -1;
            Statement stnmt = con.createStatement();
            ResultSet rs = stnmt.executeQuery("SELECT COUNT(*) FROM dwhproject.masterdata;");
            while (rs.next()) {
                counts = rs.getInt(1);
            }
            return counts;
        } catch (Exception e) {
            return -1;
        }
    }
}
