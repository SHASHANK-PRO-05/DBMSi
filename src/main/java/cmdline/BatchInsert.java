package cmdline;

import columnar.ColumnarFile;
import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.SystemDefs;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class BatchInsert implements GlobalConst {
    private static String dbFile;
    private static String columnDBName;
    private static String columnarFileName;
    private static short numColumns;

    public static void main(String argv[]) throws Exception {
        initFromArgs(argv);
    }

    /**
     * TODO: Change this to buffered reader
     */
    private static BufferedReader bufferedReader;

    private static AttrType[] parseHeader() throws Exception {

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
                attrTypes[i].setSize(4);
            } else {
                int sizeOfString = Integer.parseInt(columnType.substring(5
                        , columnType.length() - 1));
                attrTypes[i].setAttrType(0);
                attrTypes[i].setSize(sizeOfString);
            }
        }
        return attrTypes;
    }

    private static void initFromArgs(String argv[])
            throws Exception {
        String fileName = argv[0];
        bufferedReader = new BufferedReader(new FileReader(fileName));
        String columnDBName = argv[1];
        String columnarFileName = argv[2];
        int numberOfColumns = Integer.parseInt(argv[3]);
        File file = new File(fileName);
        int pageSizeRequired = (int) (file
                .length() / MINIBASE_PAGESIZE) * 4;

        int bufferSize = pageSizeRequired / 3;
        if (bufferSize < 10) bufferSize = 10;
        SystemDefs systemDefs = new SystemDefs(columnDBName, pageSizeRequired
                , bufferSize, "LRU");
        AttrType[] attrTypes = parseHeader();
        ColumnarFile columnarFile = new ColumnarFile(columnarFileName, numberOfColumns, attrTypes);
        SystemDefs.JavabaseBM.flushAllPages();
        insertRecords(columnarFile, attrTypes);

    }

    public static void insertRecords(ColumnarFile columnarFile
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
            String[] strings = arrayList.get(j).toString().split("\n");
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

            //System.out.println(columnarFile.getTupleCount());
        }
        double endTime = System.currentTimeMillis();
        double duration = (endTime - startTime);
        System.out.println(duration / 1000);
        System.out.println(SystemDefs.pCounter.getwCounter());
        SystemDefs.JavabaseBM.flushAllPages();
//        int size = 0;
//        int[] position = new int[attrTypes.length];
//        int prev = 0;
//
//        for (int i = 0; i < attrTypes.length; i++) {
//            size += attrTypes[i].getSize();
//            position[i] = prev;
//
//            prev = prev + attrTypes[i].getSize();
//        }
//        int count = 0;
//        String string = bufferedReader.readLine();
//        while (string != null) {
//            String[] s = string.split("\t");
//            byte[] bytes = new byte[size];
//            for (int i = 0; i < s.length; i++) {
//                if (attrTypes[i].getAttrType() == 1) {
//                    int value = Integer.parseInt(s[i]);
//                    Convert.setIntValue(value, position[i], bytes);
//                } else {
//                    Convert.setStringValue(s[i], position[i], bytes);
//                }
//            }
//            //System.out.println("hello");
//            columnarFile.insertTuple(bytes);
//            //System.out.println(count++);
//            string = bufferedReader.readLine();
//        }
//        System.out.println("-------------------------");
//        System.out.println(columnarFile.getTupleCount());
    }
}
