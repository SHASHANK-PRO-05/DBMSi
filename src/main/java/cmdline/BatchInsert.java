package cmdline;

import columnar.ColumnarFile;
import global.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Scanner;


public class BatchInsert implements GlobalConst {
    private String dbFile;
    private String columnDBName;
    private String columnarFileName;
    private short numColumns;
    private BufferedReader bufferedReader;

    /**
     * The main class
     *
     * @param argv
     * @throws Exception
     */
    public static void main(String argv[]) throws Exception {
        if (argv.length != 4) {
            System.out.println("--- Usage of the command ---");
            System.out.println("batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
        } else {
            BatchInsert batchInsert = new BatchInsert();
            batchInsert.initFromArgs(argv);
        }
    }


    private AttrType[] parseHeader() throws Exception {
        String header = bufferedReader.readLine();
        String columnsString[] = header.split("\t");
        AttrType[] attrTypes = new AttrType[columnsString.length];

        for (int i = 0; i < attrTypes.length; i++) {
            String[] tempString = columnsString[i].split(":");
            String columnName = tempString[0];
            String columnType = tempString[1];
            attrTypes[i] = new AttrType();
            attrTypes[i].setAttrName(columnName);
            attrTypes[i].setColumnId(i);
            if (columnType.equals("int")) {
                attrTypes[i].setAttrType(1);
                attrTypes[i].setSize((short) 4);
            } else {
                short sizeOfString = (short) Integer.parseInt(columnType.substring(5
                        , columnType.length() - 1));
                attrTypes[i].setAttrType(0);
                attrTypes[i].setSize(sizeOfString);
            }
        }
        return attrTypes;
    }

    /**
     * @param argv parameters to run the system
     * @throws Exception
     */
    private void initFromArgs(String argv[])
            throws Exception {
        String fileName = argv[0];
        File file = new File(fileName);


        //Check if file exists which needs to be inserted
        if (!file.exists() || file.isDirectory()) {
            System.out.println("** The specfied data file does not exists **");
            return;
        }
        bufferedReader = new BufferedReader(new FileReader(fileName));
        AttrType[] attrTypes;


        //Collecting the header for the insert
        try {
            attrTypes = parseHeader();
        } catch (Exception e) {
            throw new Exception("Not able to parse the header. Please check your file");
        }
        String columnDBName = argv[1];
        boolean override = true;
        if (new File(columnDBName).isFile()) {
            //If file exists take permission to overwrite or insert
            Scanner scanner = new Scanner(System.in);
            System.out.print("DB already exists. Do you want to overwrite it? (yes/no)");
            String choice = scanner.next();
            if (choice.toLowerCase().equals("yes"))
                override = true;
            else
                override = false;
        }

        //checking number of columns required for batchinsert
        int numberOfColumns = 0;
        try {
            numberOfColumns = Integer.parseInt(argv[3]);
        } catch (Exception e) {
            throw new Exception("Integer value required for number of columns");
        }

        //If length is not the same for the header.
        if (numberOfColumns != attrTypes.length) throw new Exception("Number of columns " +
                "do not match with the number of columns in header");


        //If it is override change the entire db
        int pageSizeRequired = 0;
        if (override)
            pageSizeRequired = Math.max((int) (file.length() / MINIBASE_PAGESIZE) * 100, 200000);

        int bufferSize = 4000;
        SystemDefs systemDefs = new SystemDefs(columnDBName, pageSizeRequired
                , bufferSize, "LRU" +
                "");

        String columnarFileName = argv[2];
        PageId pageId = SystemDefs.JavabaseDB.getFileEntry(columnarFileName);

        ColumnarFile columnarFile;
        if (pageId == null) {
            columnarFile = new ColumnarFile(columnarFileName, numberOfColumns, attrTypes);
        } else {
            columnarFile = new ColumnarFile(columnarFileName);
        }

        insertRecords(columnarFile, attrTypes);
        SystemDefs.JavabaseBM.flushAllPages();


    }

    public void insertRecords(ColumnarFile columnarFile
            , AttrType[] attrTypes) throws Exception {
        int size = 0;
        int[] position = new int[attrTypes.length];
        int prev = 0;
        for (int i = 0; i < attrTypes.length; i++) {
            size += attrTypes[i].getSize();
            position[i] = prev;

            prev = prev + attrTypes[i].getSize();
        }
        ArrayList<String> arrayList = new ArrayList<String>();
        String s = bufferedReader.readLine();
        while (s != null) {
            arrayList.add(s);
            s = bufferedReader.readLine();
        }
        int count = 0;
        double startTime = System.currentTimeMillis();
        for (int j = 0; j < arrayList.size(); j++) {
            String[] strings = arrayList.get(j).toString().split("\t");
            byte[] bytes = new byte[size];
            for (int i = 0; i < strings.length; i++) {
                if (attrTypes[i].getAttrType() == 1) {
                    int value = Integer.parseInt(strings[i]);
                    Convert.setIntValue(value, position[i], bytes);
                } else {
                    Convert.setStringValue(strings[i], position[i], bytes);
                }
            }
            columnarFile.insertTuple(bytes);
        }
        double endTime = System.currentTimeMillis();
        double duration = (endTime - startTime);
        System.out.println("Time taken (Seconds)" + duration / 1000);
        System.out.println("Tuples in the table now:" + columnarFile.getColumnarHeader().getReccnt());

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Write count: " + SystemDefs.pCounter.getwCounter());
        System.out.println("Read count: " + SystemDefs.pCounter.getrCounter());

    }
}
