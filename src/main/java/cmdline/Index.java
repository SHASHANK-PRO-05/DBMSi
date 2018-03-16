package cmdline;

import bitmap.BitMapFile;
import bitmap.GetFileEntryException;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import global.Convert;
import global.IndexType;
import global.IntegerValue;
import global.PageId;
import global.RID;
import global.StringValue;
import global.SystemDefs;
import global.ValueClass;
import heap.Scan;
import heap.Tuple;

public class Index {
	private static String columnDBName;
	private static String columnarFileName;
	private static int columnId;
	private static String indexType;
	private static String columnName;
	private static ColumnarFile columnarFile;

	public static void main(String argv[]) throws Exception {
		initFromArgs(argv);
	}

	private static void initFromArgs(String argv[]) throws Exception {
		columnDBName = argv[0];
		columnarFileName = argv[1];
		columnName = argv[2];
		indexType = argv[3];

	//	new SystemDefs(columnDBName, 0, 10, "LRU"); // Not sure about buffer pool size


		columnarFile = new ColumnarFile(columnarFileName);
		for (int i = 0; i < columnarFile.getColumnarHeader().getColumnCount(); i++) {
			if (columnarFile.getColumnarHeader().getColumns()[i].getAttrName().equals(columnName)) {
				columnId = columnarFile.getColumnarHeader().getColumns()[i].getColumnId();

			}
		}

		switch(indexType) {
		case ("BITMAP"):{
			generationBitmap(columnarFile);
		}
			break;
		case("BTREE"):{
			
		}
		break;
		}

		
	}

	private static void generationBitmap(ColumnarFile columnarFile) throws Exception {
		Scan scan = new Scan(columnarFile, (short) columnId);
		long count = columnarFile.getTupleCount();
		RID rid = new RID();
		Tuple tuple;
		for (int i = 0; i < count; i++) {
			String indexFileName = new String();
			tuple = scan.getNext(rid);
			IndexInfo indexInfo = new IndexInfo();
			indexInfo.setColumnNumber(columnId);
			IndexType indexType = new IndexType(3);
			ValueClass value;
			PageId pageId;
			byte[] by = tuple.getTupleByteArray();
			if (columnarFile.getColumnarHeader().getColumns()[i].getAttrType() == 0) {
				StringValue stringValue = new StringValue(Convert.getStringValue(0, by, by.length));
				indexInfo.setValue(stringValue);
				value = stringValue;
			}

			else {
				IntegerValue integerValue = new IntegerValue(Convert.getIntValue(0, by));
				indexInfo.setValue(integerValue);
				value = integerValue;
			}
			indexFileName = columnDBName + "." + columnId + "." + value.getValue().toString();
			columnarFile.setIndexFileName(indexFileName);
			indexInfo.setFileName(indexFileName);
			indexInfo.setIndexType(indexType);
			pageId = getFileEntry(indexFileName);
			if (pageId == null) {
				new BitMapFile(indexFileName,columnarFile, columnId, value);
			}

		}

		
	}

	private static PageId getFileEntry(String fileName) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.getFileEntry(fileName);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

}