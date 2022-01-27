package com.dwh;

import java.util.Formatter;

public class OutputFormatter {
    public void displayHeaderTransaction() {
        Formatter fmt = new Formatter();
        System.out.println("================================================================================================================================================");
        fmt.format("%15s %15s %15s %15s %15s %15s %15s %15s\n",
                "TRANSACTION_ID", "PRODUCT_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "STORE_ID", "STORE_NAME", "T_DATE", "QUANTITY");
        System.out.print(fmt);
        System.out.println("================================================================================================================================================");
    }

    public void displayTransaction(TransactionRecord transaction) {
        Formatter fmt = new Formatter();
        fmt.format("%15s %15s %15s %15s %15s %15s %15s %15s\n",
                transaction._id, transaction.productID, transaction.customerID, transaction.customerName, transaction.storeID, transaction.storeName, transaction.tDate, transaction.quantity);
        System.out.print(fmt);
    }

    public void displayHeaderMasterData() {
        Formatter fmt = new Formatter();
        System.out.println("============================================================================================");
        fmt.format("%15s %15s %15s %15s %15s\n",
                "PRODUCT_ID", "PRODUCT_NAME", "SUPPLIER_ID", "SUPPLIER_NAME", "PRICE");
        System.out.print(fmt);
        System.out.println("============================================================================================");
    }

    public void displayMasterData(TransactionRecord transactionRecord) {
        Formatter fmt = new Formatter();
        fmt.format("%15s %15s %15s %15s %15s\n",
                transactionRecord.productID, transactionRecord.productName, transactionRecord.supplierID, transactionRecord.supplierName, transactionRecord.price);
        System.out.print(fmt);
    }
}