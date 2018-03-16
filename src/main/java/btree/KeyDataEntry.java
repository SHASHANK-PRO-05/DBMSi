package btree;

import global.PageId;
import global.RID;
import global.TID;

public class KeyDataEntry {
	private KeyClass key;
	private DataClass data;

	public KeyDataEntry(Integer key, PageId pageNo) {
		this.key = new IntegerKey(key);
		this.data = new IndexData(pageNo);
	}

	public KeyDataEntry(String key, PageId pageNo) {
		this.key = new StringKey(key);
		this.data = new IndexData(pageNo);
	}

	public KeyDataEntry(KeyClass key, PageId pageNo) {
		data = new IndexData(pageNo);
		if (key instanceof IntegerKey)
			this.key = new IntegerKey(((IntegerKey) key).getKey());
		else if (key instanceof StringKey)
			this.key = new StringKey(((StringKey) key).getKey());
	}

	public KeyDataEntry(Integer key, TID tid) {
		this.key = new IntegerKey(key);
		this.data = new LeafData(tid);
	}

	public KeyDataEntry(String key, TID tid) {
		this.key = new StringKey(key);
		this.data = new LeafData(tid);
	}

	public KeyDataEntry(KeyClass key, DataClass data) {
		if (key instanceof IntegerKey)
			this.key = new IntegerKey(((IntegerKey) key).getKey());
		else if (key instanceof StringKey)
			this.key = new StringKey(((StringKey) key).getKey());

		if (data instanceof IndexData)
			this.data = new IndexData(((IndexData) data).getData());
		else if (data instanceof LeafData)
			this.data = new LeafData(((LeafData) data).getData());
	}

	public boolean equals(KeyDataEntry entry) {
		boolean st1, st2;

		if (key instanceof IntegerKey)
			st1 = ((IntegerKey) key).getKey().equals(((IntegerKey) entry.key).getKey());
		else
			st1 = ((StringKey) key).getKey().equals(((StringKey) entry.key).getKey());

		if (data instanceof IndexData)
			st2 = ((IndexData) data).getData().pid == ((IndexData) entry.data).getData().pid;
		else
			st2 = ((TID) ((LeafData) data).getData()).equals(((TID) ((LeafData) entry.data).getData()));

		return (st1 && st2);
	}

	public KeyDataEntry(KeyClass key, TID tid) {
		data = new LeafData(tid);
		if (key instanceof IntegerKey)
			this.key = new IntegerKey(((IntegerKey) key).getKey());
		else if (key instanceof StringKey)
			this.key = new StringKey(((StringKey) key).getKey());
	}
}
