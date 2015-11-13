package stm.transaction;

import java.util.Map;


public class TransactionContext {
	WriteSet writeset = new WriteSet();
	private ReadSet readset = new ReadSet();
	private byte[] result;
	
	private int Tid = 0;
	/* Default constructor for deserialization, Tid = 0 */
        
	public TransactionContext()
        {
                this.Tid = 0;
        }

        public TransactionContext( int Id)
        {
                this.Tid = Id;
        }
	
	public int getTransactionId()
	{
		return Tid;
	}


	public AbstractObject getLatestUnCommittedCopy(String objId){
		return writeset.getobject(objId);
	}
	
	public void setResult(byte[] result) {
		this.result = result;
	}
	
	public byte[] getResult() {
		return result;
	}
	
	public void addObjectToWriteSet(String objId, AbstractObject object) {
		writeset.addToWriteSet(objId, object);
	}
	
	public void addObjectToReadSet(String objId, AbstractObject object) {
		readset.addToReadSet(objId, object);
	}
	
	public WriteSet getShadowCopySetElement() {
		return writeset;
	}

	public Map<String, AbstractObject> getReadSet() {
		return readset.getReadSet();
	}
	
	public Map<String, AbstractObject> getWriteSet() {
		return writeset.getWriteSet();
	}
	
}
