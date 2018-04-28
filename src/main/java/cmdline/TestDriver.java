package cmdline;

import global.GlobalConst;
import global.SystemDefs;

import java.util.Scanner;

public class TestDriver {
    static String[][] arrays = {{"BatchInsert", "sample.txt Minibase.min Employee 4"},
            {"Index", "Minibase.min Employee A BITMAP"},
            {"ColumnIndexScan", "Minibase.min Employee [ ( A >= New_Hampshire OR C == 3 ) AND B == District_of_Columbia ] [ A B C D ] 40 false"},
            {"Index", "Minibase.min Employee B BTREE"},
            {"Index", "Minibase.min Employee C BITMAP"},
            {"ColumnIndexScan", "Minibase.min Employee [ ( A >= New_Hampshire OR C == 3 ) AND B == District_of_Columbia ] [ A B C D ] 40 false"},
            {"ColumnIndexScan", "Minibase.min Employee [ A == New_Hampshire AND C == 3 AND B == District_of_Columbia ] [ A B C D ] 40 false"},
            {"ColumnSortScan", "Minibase.min Employee A ASC 6"},
            {"ColumnSortScan", "Minibase.min Employee A DSC 6"},
            {"ColumnSortScan", "Minibase.min Employee C DSC 6"},
            {"ColumnSortScan", "Minibase.min Employee C ASC 6"},
            {"ColumnIndexScan", "Minibase.min Employee [ A == New_Hampshire AND C == 3 AND B == District_of_Columbia ] [ A B C D ] 40 false"}

    };

    public static void initialCases() {
        Runtime rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec("rm -rf Minibase.min");
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int i = 0; i < arrays.length; i++) {
            switch (arrays[i][0]) {
                case "BatchInsert":
                    try {
                        BatchInsert batchInsert = new BatchInsert();
                        batchInsert.main(arrays[i][1].split(" "));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "ColumnSortScan":
                    try {
                        ColumnSortScan columnSortScan = new ColumnSortScan();
                        columnSortScan.main(arrays[i][1].split(" "));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "Index":
                    try {
                        Index index = new Index();
                        index.main(arrays[i][1].split(" "));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "ColumnIndexScan":
                    try {
                        ColumnIndexScan columnIndexScan = new ColumnIndexScan();
                        columnIndexScan.main(arrays[i][1].split(" "));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }
        }
        rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec("rm -rf Minibase.min");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        initialCases();
        Scanner scanner = new Scanner(System.in);
        System.out.println("Welcome to Minibase Test Driver(Page Size " + GlobalConst.MINIBASE_PAGESIZE + ")");
        boolean loop = true;
        while (loop) {
            System.out.println("---------------------------------------------------------------");
            System.out.println("The supported features are:- " +
                    "\n1) BatchInsert" +
                    "\n2) Index (BitMap, BTree)" +
                    "\n3) Query on Single Table (Supported indexes Bitmap, Btree, ColumnScan)" +
                    "\n4) Column Sort" +
                    "\n5) Exit");
            System.out.print("Enter your choice: ");
            int number = Integer.parseInt(scanner.nextLine());
            String command;
            String[] commandArray;
            switch (number) {
                case 1:
                    BatchInsert batchInsert = new BatchInsert();
                    System.out.println("Usage: <<text file>> <<DB Name>> <<Table Name>> <<Number of Columns>>");
                    command = scanner.nextLine();
                    commandArray = command.split(" ");
                    try {
                        batchInsert.main(commandArray);
                        SystemDefs.JavabaseBM.flushAllPages();
                        SystemDefs.JavabaseDB.closeDB();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    break;
                case 2:
                    Index index = new Index();
                    System.out.println("Usage: <<DB Name>> <<Table Name>> <<ColumnName>> <<IndexType (Btree or Bitmap)>>");
                    command = scanner.nextLine();
                    commandArray = command.split(" ");
                    try {
                        index.main(commandArray);
                        SystemDefs.JavabaseBM.flushAllPages();
                        SystemDefs.JavabaseDB.closeDB();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 3:
                    try {
                        ColumnIndexScan columnarIndexScan = new ColumnIndexScan();
                        System.out.println("Usage: <<DBName>> <<TableName>> <<CNF Query>> <<Projection>> <<Buffer Size>> <<Interaction Required(true/false)>>");
                        command = scanner.nextLine();
                        commandArray = command.split(" ");
                        try {
                            columnarIndexScan.main(commandArray);
                            SystemDefs.JavabaseBM.flushAllPages();
                            SystemDefs.JavabaseDB.closeDB();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 4:
                    try {
                        ColumnSortScan columnSortScan = new ColumnSortScan();
                        System.out.println("Usage: <<DB Name>> <<Table Name>> <<Column Name>> <<Order>> <<Buffer Size>>");
                        command = scanner.nextLine();
                        commandArray = command.split(" ");
                        try {
                            columnSortScan.main(commandArray);
                            SystemDefs.JavabaseBM.flushAllPages();
                            SystemDefs.JavabaseDB.closeDB();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 5:
                    loop = false;
                    break;
                default:
                    System.out.println("Please provide a valid number");
            }
        }
    }

}
