package iterator;

import bitmap.BitMapFile;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import global.*;

import java.util.ArrayList;
import java.util.UUID;

public class BitMapScanUtil implements LateMaterializationUtil {
    private String relName;
    private ColumnarFile columnarFile;
    private CondExpr[] condExprs;
    AttrType attrType;
    ArrayList<BitMapFile> bitMapFiles[];

    ArrayList<BitMapFile> filesToDelete = new ArrayList<>();

    public BitMapScanUtil(String relName,
                          AttrType attrType, CondExpr[] condExprs) throws Exception {
        this.columnarFile = new ColumnarFile(relName);
        this.attrType = attrType;
        this.condExprs = condExprs;
    }

    @Override
    public ArrayList<BitMapFile>[] makeBitMapFile() throws Exception {
        ArrayList<IndexInfo> indexInfos = columnarFile.getColumnarHeader()
                .getParticularTypeIndex(attrType.getColumnId(), new IndexType(IndexType.BitMapIndex));
        long tupleCount = columnarFile.getTupleCount();
        bitMapFiles = new ArrayList[condExprs.length];
        for (int i = 0; i < condExprs.length; i++) {
            CondExpr iter = condExprs[i];
            bitMapFiles[i] = new ArrayList<>();
            while (iter != null) {
                if (iter.operand1.symbol.offset == attrType.getColumnId()) {
                    ValueClass valueClass;
                    if (attrType.getAttrType() == AttrType.attrInteger) {
                        valueClass = new IntegerValue(iter.operand2.integer);
                    } else {
                        valueClass = new StringValue(iter.operand2.string);
                    }
                    for (IndexInfo indexInfo : indexInfos) {
                        switch (iter.op.attrOperator) {
                            case AttrOperator.aopEQ:
                                if (indexInfo.getValue().compare(valueClass) == 0) {
                                    bitMapFiles[i].add(new BitMapFile(indexInfo.getFileName()));
                                }
                                break;
                            case AttrOperator.aopGT:
                                if (indexInfo.getValue().compare(valueClass) > 0) {
                                    bitMapFiles[i].add(new BitMapFile(indexInfo.getFileName()));
                                }
                                break;
                            case AttrOperator.aopLT:
                                if (indexInfo.getValue().compare(valueClass) < 0) {
                                    bitMapFiles[i].add(new BitMapFile(indexInfo.getFileName()));
                                }
                                break;
                            case AttrOperator.aopLE:
                                if (indexInfo.getValue().compare(valueClass) <= 0) {
                                    bitMapFiles[i].add(new BitMapFile(indexInfo.getFileName()));
                                }
                                break;
                            case AttrOperator.aopGE:
                                if (indexInfo.getValue().compare(valueClass) >= 0) {
                                    bitMapFiles[i].add(new BitMapFile(indexInfo.getFileName()));
                                }
                                break;
                            case AttrOperator.aopNE:
                                if (indexInfo.getValue().compare(valueClass) != 0) {
                                    bitMapFiles[i].add(new BitMapFile(indexInfo.getFileName()));
                                }
                                break;
                        }
                    }
                } else {
                    BitMapFile bitMapFile = new BitMapFile(UUID.randomUUID().toString(), false);
                    filesToDelete.add(bitMapFile);
                    bitMapFile.Delete((int) tupleCount);
                    bitMapFiles[i].add(bitMapFile);
                }
                iter = iter.next;
            }
        }
        for (int i = 0; i < condExprs.length; i++) {
            if (bitMapFiles[i].size() == 0) {
                BitMapFile tempBitMap = new BitMapFile(UUID.randomUUID().toString(), false);
                tempBitMap.Delete((int) tupleCount);
                bitMapFiles[i].add(tempBitMap);
                filesToDelete.add(tempBitMap);
            }
        }
        return bitMapFiles;
    }

    @Override
    public void destroyEveryThing() throws Exception {
        for (BitMapFile bitMapFile : filesToDelete) {
            bitMapFile.destroyBitMapFile();
        }
    }
}
