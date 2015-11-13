package stm.impl.objectstructure;

import stm.transaction.AbstractObject;
import stm.impl.LockObject;

public class SharedObject {
	
	private CommittedObjects committedObjects; 
	private CompletedObject completedObject; 
	
	/*Adding the lock object */
	private LockObject objLock;
	
	public SharedObject(AbstractObject object) {
		committedObjects = new CommittedObjects(object);
		completedObject = new CompletedObject();	
		objLock = new LockObject();
	}
	
	public AbstractObject getLatestCommittedObject(int transactionSnapshot) {
		return committedObjects.getLatestObject(transactionSnapshot);
	}
	
	public AbstractObject getLatestCommittedObject() {
		return committedObjects.getLatestObject();
	}
	
	public AbstractObject getLatestCompletedObject() {
		return completedObject.getCurrentObject();
	}
	
	public void updateCompletedObject(AbstractObject object) {
		completedObject.setCurrentObject(object);
	}
	
	public void updateCommittedObject(AbstractObject object, int timeStamp) {
		committedObjects.addCommittedObject(object, timeStamp);
	}

	/*Methods to lock and unlock*/
	
	public void ObjectLock(String Id)
	{
		while(!objLock.lock(Id))
		{
			;
		}
		return;
	}

	public void ObjectUnlock(String Id)
	{
		objLock.unlock(Id);
	}

}
