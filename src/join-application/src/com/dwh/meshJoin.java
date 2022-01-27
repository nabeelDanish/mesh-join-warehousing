package com.dwh;

import com.dwh.SQL.InputConnection;
import com.dwh.SQL.OutputConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;

public class meshJoin {

    // Main function
    public static void main(String[] args) {
        // Usernames and passwords
        String username = "root";
        String password = "Gaming4life";

        // SQL Connections
        InputConnection inputConnection = new InputConnection("jdbc:mysql://localhost:3306/dwhproject", username, password);
        OutputConnection outputConnection = new OutputConnection("jdbc:mysql://localhost:3306/dw_project", username, password);

        // Terminal I/O Classes
        OutputFormatter outputFormatter = new OutputFormatter();
        Scanner scanner = new Scanner(System.in);

        // Previous Operation Information
        int prevLoaded;
        System.out.print("Please enter the number of records already in the Warehouse: ");
        prevLoaded = scanner.nextInt();

        // Metadata before the mesh-join
        int choice = 1;
        int currMDOffset = 0;
        int currTOffset = 0;
        int transactionBlockSize = 50;
        int mdBlockSize = 10;
        int totalMDSize = -1;
        int totalTransactionSize = -1;
        int totalLoaded = prevLoaded;
        int totalJoined = 0;
        int numMDCycles;

        // Main Data structures used
        HashMap<String, TransactionRecord> masterData;
        HashMap<String, ArrayList<TransactionRecord>> transactionHashMap = new HashMap<>();
        LinkedList<ArrayList<TransactionRecord>> queue = new LinkedList<>();

        // Main Services
        while (true) {
            System.out.println("Press 1 to perform ETL\nPress 0 to Terminate");
            choice = scanner.nextInt();
            if (choice == 0)
                break;

            // Fetching metadata
            totalMDSize = inputConnection.getMDCount();
            totalTransactionSize = inputConnection.getTransactionsCount();

            currTOffset = totalLoaded;
            numMDCycles = (totalMDSize / mdBlockSize);

            while (totalJoined < totalTransactionSize) {
                // Step 1. Loading the 50 Transaction data
                ArrayList<TransactionRecord> temp = inputConnection.retrieveRecord(currTOffset, transactionBlockSize);
                ArrayList<TransactionRecord> queueRecords = new ArrayList<>();

                for (TransactionRecord record : temp) {
                    // Adding in Transaction Hashmap
                    if (transactionHashMap.containsKey(record.productID)) {
                        transactionHashMap.get(record.productID).add(record);
                    } else {
                        ArrayList<TransactionRecord> list = new ArrayList<>();
                        list.add(record);
                        transactionHashMap.put(record.productID, list);
                    }

                    // Adding in Queue Hashmap
                    queueRecords.add(record);
                }
                queue.add(queueRecords);

                currTOffset = (currTOffset + transactionBlockSize);

                // Step 2. Reading Master Data Disk
                masterData = inputConnection.retrieveMasterDataBlock(currMDOffset, mdBlockSize);
                currMDOffset = (currMDOffset + mdBlockSize) % totalMDSize;

                // Step 3. Tuple Lookup
                for (String key : masterData.keySet()) {
                    // Fetching transaction tuples
                    TransactionRecord mRecord = masterData.get(key);

                    if (transactionHashMap.containsKey(key)) {

                        ArrayList<TransactionRecord> records = transactionHashMap.get(key);
                        for (TransactionRecord record : records) {
                            record.productName = mRecord.productName;
                            record.supplierID = mRecord.supplierID;
                            record.supplierName = mRecord.supplierName;
                            record.price = mRecord.price;
                            record.totalSale = record.quantity * record.price;

                            ++totalJoined;

                            if (totalJoined % 500 == 0) {
                                System.out.print(totalJoined);
                                System.out.println(" Records Joined ...");
                            }
                        }
                    }
                }

                // Step 6. Removing Everything from the last chunk
                if (queue.size() == mdBlockSize) {
                    ArrayList<TransactionRecord> removeData = queue.remove();
                    for (TransactionRecord record : removeData) {
                        // Verbose
                        ++totalLoaded;
                        if (totalLoaded % 500 == 0) {
                            System.out.print(totalLoaded);
                            System.out.println(" Records Loaded ...");
                        }

                        // Step 5. Loading into the warehouse
                        outputConnection.loadIntoWarehouse(record);
                        transactionHashMap.get(record.productID).remove(record);
                    }
                }
            }

            // Step 6. Removing Everything from the last chunk
            ArrayList<TransactionRecord> removeData = queue.remove();
            for (TransactionRecord record : removeData) {
                // Verbose
                ++totalLoaded;
                if (totalLoaded % 500 == 0) {
                    System.out.print(totalLoaded);
                    System.out.println(" Records Loaded ...");
                }

                // Step 5. Loading into the warehouse
                outputConnection.loadIntoWarehouse(record);
                transactionHashMap.get(record.productID).remove(record);
            }

            System.out.print(totalLoaded);
            System.out.println(" Records Loaded ...\nAll Data Loaded!");
        }

        // Closing Connections
        inputConnection.closeConnection();
        outputConnection.closeConnection();
    }
}
