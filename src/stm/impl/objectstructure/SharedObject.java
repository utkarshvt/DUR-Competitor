package stm.impl.objectstructure;

import stm.transaction.AbstractObject;
import stm.impl.LockObject;
import java.util.concurrent.atomic.AtomicInteger;

import lsr.common.ClientRequest;
import lsr.common.Request;
import lsr.common.RequestId;


public class SharedObject {
	
	private CommittedObjects committedObjects; 
	private CompletedObject completedObject; 
	
	/*Adding the lock object */
	//private LockObject objLock;
	AtomicInteger owner;	

	public SharedObject(AbstractObject object) {
		committedObjects = new CommittedObjects(object);
		completedObject = new CompletedObject();	
		//objLock = new LockObject();
		owner = new AtomicInteger(0);
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
	
	public void ObjectLock(String Id, RequestId requestId, int Tid)
	{
		int curr = 0;
                int it = 0;
                boolean locked = false;
                //System.out.println("Request ClientId " + requestId.getClientId() + " SeqNumber " + requestId.getSeqNumber() +  " Tid = " + Tid + " Trying to lock " + Id);

		
		while(!locked)
                {
                        curr = owner.get(); 
                 	if(curr == 0 || curr == Tid)
			{
				if( this.owner.compareAndSet(curr, Tid) == true)
                 		{
                         		//System.out.println("Iterations in while = " + it + " Owner = " + caller + " Prev owner = " + curr);
                        		//System.out.println("Request ClientId " + requestId.getClientId() + " SeqNumber " + requestId.getSeqNumber() + " Locked " + Id);
					locked = true;        
                        
               			}
          		}
                 	else
			{
				try
                        	{
                                	Thread.sleep(1);
                        	}
                        	catch (InterruptedException ex)
                        	{
                                	ex.printStackTrace();
                        	}
	
			}       
		}
	}

	public void ObjectUnlock(String Id, RequestId requestId, int Tid)
	{
		//System.out.println("Trying to unlock " + Id + " Tid is " + Tid);
		if(Tid == 0)
			return;
		//if(this.owner.get() == 1)
		//	System.out.println("Going to unlock the locked object " + Id);
		if(this.owner.compareAndSet(Tid,0) == true)
		{
			//System.out.println("True unlock");
		}
		//System.out.println("Request ClientId " + requestId.getClientId() + " SeqNumber " + requestId.getSeqNumber() + "Tid = " + Tid + " UnLocked " + Id);
	}


	public boolean ObjectTrylock(String Id, int Tid)
	{
		if(this.owner.get() == Tid)
			return true;

		return(this.owner.compareAndSet(0, Tid));
	}

	public int getOwner()
	{
		return this.owner.get();
	}

}
