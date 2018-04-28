package cmdline;

import columnar.ByteToTuple;
import columnar.ColumnarFile;
import global.*;
import heap.Tuple;
import iterator.ColumnarIndexScan;
import iterator.CondExpr;
import iterator.FldSpec;

import java.util.*;

public class ColumnIndexScan {
    public ColumnarFile columnarFile;
    public AttrType[] attrTypes;
    public AttrType[] requiredAttrTypes;
    public ArrayList<CondExpr> condExprs = new ArrayList<>();
    public ArrayList<FldSpec> fldSpecArrayList = new ArrayList<>();
    public Set<String> columnNames = new HashSet<>();
    public IndexType[] indexTypes;
    public String relName;
    public AttrType[] projectionBreak;

    public static void main(String[] args) throws Exception {
        //(DBName) (TableName) [ CNF Query ] [ Projection ] (Buffer PoolSize)
        ColumnIndexScan columnIndexScan = new ColumnIndexScan();
        columnIndexScan.initFromArgs(args);
        columnIndexScan.begin();

    }

    public void begin() throws Exception {
        CondExpr[] cond = condExprs.toArray(new CondExpr[condExprs.size()]);

        ColumnarIndexScan columnarIndexScan = new ColumnarIndexScan(relName,
                null, indexTypes, null, requiredAttrTypes, null, -1, -1, fldSpecArrayList.toArray(new FldSpec[fldSpecArrayList.size()]), condExprs.toArray(new CondExpr[condExprs.size()]));
        ByteToTuple byteToTuple = new ByteToTuple(projectionBreak);
        String ans2 = "Count";
        System.out.print(ans2);
        int temp2 = 25 - ans2.length();
        for (int j = 0; j < temp2; j++)
            System.out.print(" ");
        for (int i = 0; i < projectionBreak.length; i++) {
            String ans = projectionBreak[i].getAttrName();
            System.out.print(ans);
            int temp = 25 - ans.length();
            for (int j = 0; j < temp; j++)
                System.out.print(" ");

        }
        System.out.println();

        int counter = 1;

        Tuple tuple = columnarIndexScan.getNext();
        while (tuple != null) {
            ArrayList<byte[]> tuples = byteToTuple.setTupleBytes(tuple.getTupleByteArray());
            String ans1 = counter + "";
            System.out.print(ans1);
            int temp1 = 25 - ans1.length();
            for (int j = 0; j < temp1; j++)
                System.out.print(" ");

            counter++;
            for (int i = 0; i < projectionBreak.length; i++) {
                if (projectionBreak[i].getAttrType() == AttrType.attrString) {
                    String ans = Convert.getStringValue(0, tuples.get(i), projectionBreak[i].getSize());
                    System.out.print(ans);
                    int temp = 25 - ans.length();
                    for (int j = 0; j < temp; j++)
                        System.out.print(" ");
                } else {
                    int ans = Convert.getIntValue(0, tuples.get(i));
                    System.out.print(ans);
                    int temp = 25 - (ans + "").length();
                    for (int j = 0; j < temp; j++)
                        System.out.print(" ");
                }
            }
            System.out.println();
            tuple = columnarIndexScan.getNext();
        }


        columnarIndexScan.close();
        System.out.println("Tuples in the table now:" + columnarFile.getTupleCount());
        System.out.println("Write count: " + SystemDefs.pCounter.getwCounter());
        System.out.println("Read count: " + SystemDefs.pCounter.getrCounter());
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    private AttrOperator parseOperator(String expr) {
        if (expr.equals("=="))
            return new AttrOperator(AttrOperator.aopEQ);
        else if (expr.equals("<="))
            return new AttrOperator(AttrOperator.aopLE);
        else if (expr.equals(">="))
            return new AttrOperator(AttrOperator.aopGE);
        else if (expr.equals("<"))
            return new AttrOperator(AttrOperator.aopLT);
        else if (expr.equals(">"))
            return new AttrOperator(AttrOperator.aopGT);
        else if (expr.equals("!="))
            return new AttrOperator(AttrOperator.aopNE);
        else
            return null;
    }

    public static Stack<String> stack;

    public void initFromArgs(String[] args) throws Exception {
        SystemDefs systemDefs = new SystemDefs(args[0], 0, Integer.parseInt(args[args.length - 2]), "LRU");
        columnarFile = new ColumnarFile(args[1]);
        relName = args[1];
        attrTypes = columnarFile.getColumnarHeader().getColumns();
        stack = new Stack<>();
        boolean secondProcessing = false;
        for (int i = 2; i < args.length - 2; i++) {
            if (!secondProcessing) {
                switch (args[i]) {
                    case "[":
                    case "(":
                    default:
                        stack.push(args[i]);
                        break;
                    case ")":
                        CondExpr head = null;
                        while (!stack.peek().equals("(")) {
                            head = parse(stack, head);
                            if (stack.peek().equals("OR")) {
                                stack.pop();
                            }
                        }
                        stack.pop();
                        condExprs.add(head);
                        if (stack.peek().equals("AND"))
                            stack.pop();
                        break;
                    case "]":
                        while (!stack.peek().equals("[")) {
                            condExprs.add(parse(stack, null));
                            if (stack.peek().equals("AND"))
                                stack.pop();
                        }
                        stack.pop();
                        secondProcessing = true;
                        break;
                }
            } else {
                switch (args[i]) {
                    case "[":
                    case "]":
                        break;
                    default:
                        columnNames.add(args[i]);
                        for (AttrType attrType : attrTypes) {
                            if (attrType.getAttrName().equals(args[i]))
                                fldSpecArrayList.add(new FldSpec(null, attrType.getColumnId()));
                        }
                        projectionBreak = new AttrType[fldSpecArrayList.size()];
                        int iter = 0;
                        for (FldSpec fldSpec : fldSpecArrayList) {
                            projectionBreak[iter++] = attrTypes[fldSpec.offset];
                        }
                        break;
                }
            }
        }
        requiredAttrTypes = new AttrType[columnNames.size()];
        int i = 0;
        for (String string : columnNames) {
            for (AttrType attrType : attrTypes) {
                if (attrType.getAttrName().equals(string)) {
                    requiredAttrTypes[i++] = attrType;
                }
            }
        }
        parseIterators(condExprs.toArray(new CondExpr[condExprs.size()]));

        if (Boolean.parseBoolean(args[args.length - 1])) {
            validateInteractiveIndexing();
        }
    }

    public CondExpr parse(Stack<String> stack, CondExpr head) throws Exception {
        String value = stack.pop();
        AttrOperator attrOperator = parseOperator(stack.pop());
        String column = stack.pop();
        columnNames.add(column);
        CondExpr tempCondExpr = new CondExpr();
        tempCondExpr.op = attrOperator;
        AttrType thisAttrType = null;
        for (AttrType attrType : attrTypes) {
            if (attrType.getAttrName().equals(column)) {
                thisAttrType = attrType;
                break;
            }
        }
        if (thisAttrType == null)
            throw new Exception("Not a valid attribute in condition");
        if (attrOperator == null)
            throw new Exception("Operator not valid");
        if (thisAttrType.getAttrType() == AttrType.attrInteger) {
            tempCondExpr.operand2.integer = Integer.parseInt(value);
        } else {
            tempCondExpr.operand2.string = value;
        }
        tempCondExpr.operand1.symbol = new FldSpec(null, thisAttrType.getColumnId());
        tempCondExpr.next = head;
        head = tempCondExpr;
        return head;
    }

    public void validateInteractiveIndexing() {
        System.out.println("This is an interactive mode for chosing index for a query");
        System.out.println("PS: make sure that indexing has been done by you else " +
                "it may fail or maybe it can corrupt your database so choose accordingly");
        for (int i = 0; i < requiredAttrTypes.length; i++) {
            System.out.println("For column " + requiredAttrTypes[i].getAttrName() + " we choose indextype: " + indexTypes[i]);
            System.out.println("Enter your Choice\n" +
                    IndexType.B_Index + ") BTree Index\n" +
                    IndexType.BitMapIndex + ") BitMap Index\n" +
                    IndexType.ColumnScan + ") Column Scan\n" +
                    "-1/Anything else) Don't change anything");
            Scanner scanner = new Scanner(System.in);
            int choice = scanner.nextInt();
            switch (choice) {
                case IndexType.B_Index:
                    indexTypes[i] = new IndexType(IndexType.B_Index);
                    break;
                case IndexType.BitMapIndex:
                    indexTypes[i] = new IndexType(IndexType.BitMapIndex);
                    break;
                case IndexType.ColumnScan:
                    indexTypes[i] = new IndexType(IndexType.ColumnScan);
                    break;
                default:
                case -1:
                    break;
            }
        }

    }

    public void parseIterators(CondExpr[] condExprs) throws Exception {
        PriorityQueue<Integer>[] priorityQueues = new PriorityQueue[requiredAttrTypes.length];
        indexTypes = new IndexType[requiredAttrTypes.length];

        for (int i = 0; i < condExprs.length; i++) {
            CondExpr condExpr = condExprs[i];
            while (condExpr != null) {
                int offset = condExpr.operand1.symbol.offset;
                if (priorityQueues[offset] == null) {
                    priorityQueues[offset] = new PriorityQueue<>();
                }
                if (condExpr.op.attrOperator == AttrOperator.aopEQ) {
                    //In equality Bitmap will get the priority
                    if (columnarFile.getColumnarHeader().getParticularTypeIndex(offset, new IndexType(IndexType.BitMapIndex)).size() != 0) {
                        priorityQueues[offset].add(IndexType.BitMapIndex);
                    } else if (columnarFile.getColumnarHeader().getParticularTypeIndex(offset, new IndexType(IndexType.B_Index)).size() != 0) {
                        priorityQueues[offset].add(IndexType.B_Index);
                    } else {
                        priorityQueues[offset].add(IndexType.ColumnScan);
                    }
                } else {
                    // Possible range queries should have B+ tree as priority
                    if (columnarFile.getColumnarHeader().getParticularTypeIndex(offset, new IndexType(IndexType.B_Index)).size() != 0) {
                        priorityQueues[offset].add(IndexType.B_Index);
                    } else if (columnarFile.getColumnarHeader().getParticularTypeIndex(offset, new IndexType(IndexType.BitMapIndex)).size() != 0) {
                        priorityQueues[offset].add(IndexType.BitMapIndex);
                    } else {
                        priorityQueues[offset].add(IndexType.ColumnScan);
                    }
                }
                condExpr = condExpr.next;
            }
        }
        for (int i = 0; i < requiredAttrTypes.length; i++) {
            if (priorityQueues[i] != null) {
                if (priorityQueues[i].size() != 0)
                    indexTypes[i] = new IndexType(priorityQueues[i].poll());
            } else {
                indexTypes[i] = new IndexType(IndexType.ColumnScan);
            }
        }
    }
}
