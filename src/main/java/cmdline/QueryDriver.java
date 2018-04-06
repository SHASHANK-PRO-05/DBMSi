package cmdline;

import columnar.ColumnarFile;
import global.AttrType;
import global.SystemDefs;
import iterator.CondExpr;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Scanner;

public class QueryDriver {
    private static Scanner scanner;

    public static void main(String[] args) throws Exception {
        System.out.println("Welcome to Columnar Minibase!!");
        System.out.println("Disclaimer: This is just a query driver so we expect that you have already inserted your data in the respective table.\n" +
                "Also make sure if you want to use some indexing techniques, you have created those already.\n" +
                "This Driver is only responsible for Phase 3 querying.");
        scanner = new Scanner(System.in);
        System.out.print("Enter the Column DB name: ");
        String columnDBName = scanner.next();
        System.out.print("Enter the buffer size you want to allocate: ");
        int bufferSize = scanner.nextInt();
        System.out.print("Enter the buffer manager algorithm: ");
        String bufferManagerAlgorithm = scanner.next();
        SystemDefs systemDefs = new SystemDefs(columnDBName, 0, bufferSize, bufferManagerAlgorithm);
        int choice = -1;

        while (true) {
            System.out.println("What would you like to do\n" +
                    "1> Sorting\n" +
                    "2> Nested Loop Join\n" +
                    "3> BitMap Equi Join\n" +
                    "4> Exit\n");
            System.out.print("Enter your choice: ");
            choice = scanner.nextInt();
            switch (choice) {
                case 4:
                    System.exit(0);
                    break;
                case 3:
                    setupBitMapEquiJoin();
                    break;
                default:
                    System.out.println("\n\n****************Not a valid choice********************\n\n");
                    break;
                case 1:
                    throw new NotImplementedException();

            }
        }
    }

    /***************************************************
     * This function will setup ColumnIndexScan iterator
     **************************************************/
    public static void setupColumnIndexScan() throws Exception {
        System.out.print("Enter the relation name: ");
        String relName = scanner.next();
        ColumnarFile columnarFile;
        try {
            columnarFile = new ColumnarFile(relName);
        } catch (Exception e) {
            throw new Exception("This columnar file does not exists!!!");
        }
        AttrType[] attrTypes = columnarFile.getColumnarHeader().getColumns();

        System.out.println("This file contains following columns");

        for (int i = 0; i < attrTypes.length; i++) {
            System.out.print(attrTypes[i].getAttrName() + "-(Index:" + attrTypes[i].getColumnId() + ", Size:" +
                    "" + attrTypes[i].getSize() + ") ");
        }

        System.out.print("\n Which columns you want to project in the query?\n\n" +
                "Please enter comma separted column numbers: ");

        String[] columnIndexes = scanner.next().split(",");
        AttrType[] projection = new AttrType[columnIndexes.length];
        for (int i = 0; i < columnIndexes.length; i++) {
            projection[i] = attrTypes[Integer.parseInt(columnIndexes[i])];
        }
        CondExpr[] condExprs = new CondExpr[attrTypes.length];
        System.out.print("Do you have any complex rules to add?(Y/N): ");
        String choice = scanner.next();
        while (choice.compareTo("Y") == 0) {
            System.out.print("For which column is this, enter the id: ");
            int columnId = scanner.nextInt();

            System.out.print("Do you have any complex rules to add?(Y/N): ");
            choice = scanner.next();
        }
    }

    public static void setupBitMapEquiJoin() throws Exception {
        System.out.println("This wizard will help you in setting up the BitMap Equi Join query.\n\n");
        System.out.println("Setup outer table\n");
        setupColumnIndexScan();
    }
}
