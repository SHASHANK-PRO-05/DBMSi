package cmdline;

import java.nio.ByteBuffer;

import bitmap.GetFileEntryException;
import columnar.ColumnarFile;
import columnar.IndexInfo;
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

		// Not sure how to use columnDB name as I dont know how to calculate the other
		// parameters like
		// pageSizeRequired and bufferSize
		/*
		 * int pageSizeRequired = (int) (file .length() / MINIBASE_PAGESIZE) * 4;
		 *
		 * int bufferSize = pageSizeRequired / 4; if (bufferSize < 10) bufferSize = 10;
		 *
		 * SystemDefs systemDefs = new SystemDefs(columnDBName, pageSizeRequired ,
		 * bufferSize, null);
		 */

		if (Integer.parseInt(indexType) == 3) {
			columnarFile = new ColumnarFile(columnarFileName);
			for (int i = 0; i < columnarFile.getColumnarHeader().getColumnCount(); i++) {
				if (columnarFile.getColumnarHeader().getColumns()[i].getAttrName().equals(columnName)) {
					columnId = columnarFile.getColumnarHeader().getColumns()[i].getColumnId();

				}
			}

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
				int length = tuple.getLength() - tuple.getOffset(); // assuming no header in tuple
				byte[] by = new byte[length];
				System.arraycopy(tuple.returnTupleByteArray(), tuple.getOffset(), by, 0, length);
				if (columnarFile.getColumnarHeader().getColumns()[i].getAttrType() == 0) {
					StringValue stringValue = new StringValue(by.toString());
					indexInfo.setValue(stringValue);
					value = stringValue;
					indexFileName = columnDBName + "." + columnName + "." + by.toString();
				}

				else {
					ByteBuffer bb = ByteBuffer.wrap(by);
					IntegerValue integerValue = new IntegerValue(bb.getInt());
					indexInfo.setValue(integerValue);
					value = integerValue;
					indexFileName = columnDBName + "." + columnName + "." + Integer.toString(bb.getInt());
				}
				columnarFile.setIndexFileName(indexFileName);
				indexInfo.setFileName(indexFileName);
				indexInfo.setIndexType(indexType);
				pageId = getFileEntry(indexFileName);
				if (pageId == null) {
					columnarFile.createBitMapIndex(columnId, value);
					columnarFile.getColumnarHeader().setIndex(indexInfo);
				}

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