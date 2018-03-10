package global;

public class TID extends java.lang.Object {
    int numRIDs;
    long position;
    RID[] recordIDs;

    public TID(int numRIDs) {
        this.numRIDs = numRIDs;
    }

    public TID(int numRIDs, int position) {
        this.numRIDs = numRIDs;
        this.position = position;
    }

    public TID(int numRIDs, long position, RID[] recordIDs) {
        this.numRIDs = numRIDs;
        this.position = position;
        this.recordIDs = recordIDs;
    }

    void copyTid(TID tid) {
        this.numRIDs = tid.numRIDs;
        this.position = tid.position;
        this.recordIDs = tid.recordIDs;
    }

    boolean equals(TID tid) {
        int i = 0;
        if (tid == null)
            return false;
		if (numRIDs == tid.numRIDs && position == tid.position) {
			for (RID rid : recordIDs) {
				if (tid.recordIDs[i] != null) {
					if (rid.slotNo == tid.recordIDs[i].slotNo && rid.pageNo.pid == tid.recordIDs[i].pageNo.pid) {
						i++;
						continue;
					} else
						return false;
				} else
					return false;
			}
			return true;
		}
		return false;
	}

    void writeToByteArray(byte[] array, int offset) {

    }

    void setPosition(int position) {
        this.position = position;
    }

    void setRID(int column, RID recordID) {
        recordIDs[column - 1] = recordID;
    }

	public int getNumRIDs() {
		return numRIDs;
	}

	public void setNumRIDs(int numRIDs) {
		this.numRIDs = numRIDs;
	}

	public RID[] getRecordIDs() {
		return recordIDs;
	}

	public void setRecordIDs(RID[] recordIDs) {
		this.recordIDs = recordIDs;
	}

}
