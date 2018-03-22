package iterator;

import btree.BTreeFile;
import btree.IndexFile;
import btree.IndexFileScan;
import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import global.AttrOperator;
import global.AttrType;

public class BtreeUtils {

	public static IndexFileScan BTree_scan(CondExpr[] selects, IndexFile indFile) throws Exception {
		IndexFileScan indScan;
		if (selects == null || selects[0] == null) {
			indScan = ((BTreeFile) indFile).new_scan(null, null);
			return indScan;
		}
		// If the condition is not correct
		if (selects[1] == null) {
			if (selects[0].type1.getAttrType() != AttrType.attrSymbol
					&& selects[0].type2.getAttrType() != AttrType.attrSymbol) {
				throw new Exception("IndexUtils.java: Invalid selection condition");
			}
		}
		KeyClass key;
		if (selects[0].op.attrOperator == AttrOperator.aopEQ) {
			if (selects[0].type1.getAttrType() != AttrType.attrSymbol) {
				key = getValue(selects[0], selects[0].type1, 1);
				indScan = ((BTreeFile) indFile).new_scan(key, key);
			} else {
				key = getValue(selects[0], selects[0].type2, 2);
				indScan = ((BTreeFile) indFile).new_scan(key, key);
			}
			return indScan;
		}
		if (selects[0].op.attrOperator == AttrOperator.aopLT || selects[0].op.attrOperator == AttrOperator.aopLE) {
			if (selects[0].type1.getAttrType() != AttrType.attrSymbol) {
				key = getValue(selects[0], selects[0].type1, 1);
				indScan = ((BTreeFile) indFile).new_scan(null, key);
			} else {
				key = getValue(selects[0], selects[0].type2, 2);
				indScan = ((BTreeFile) indFile).new_scan(null, key);
			}
			return indScan;
		}
		if (selects[0].op.attrOperator == AttrOperator.aopGT || selects[0].op.attrOperator == AttrOperator.aopGE) {
			if (selects[0].type1.getAttrType() != AttrType.attrSymbol) {
				key = getValue(selects[0], selects[0].type1, 1);
				indScan = ((BTreeFile) indFile).new_scan(key, null);
			} else {
				key = getValue(selects[0], selects[0].type2, 2);
				indScan = ((BTreeFile) indFile).new_scan(key, null);
			}
			return indScan;
		}
		System.err.println("Error -- in IndexUtils.BTree_scan()");
		return null;

	}

	private static KeyClass getValue(CondExpr cd, AttrType type, int choice) throws UnknownKeyTypeException {
		// error checking
		if (cd == null) {
			return null;
		}
		if (choice < 1 || choice > 2) {
			return null;
		}

		switch (type.getAttrType()) {
		case AttrType.attrString:
			if (choice == 1)
				return new StringKey(cd.operand1.string);
			else
				return new StringKey(cd.operand2.string);
		case AttrType.attrInteger:
			if (choice == 1)
				return new IntegerKey(new Integer(cd.operand1.integer));
			else
				return new IntegerKey(new Integer(cd.operand2.integer));
		default:
			throw new UnknownKeyTypeException("IndexUtils.java: Only Integer and String keys are supported so far");
		}

	}

}
