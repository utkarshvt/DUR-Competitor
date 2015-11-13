package stm.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.*;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;


import com.esotericsoftware.kryo.Kryo;

import stm.transaction.AbstractObject;
import stm.transaction.TransactionContext;
import stm.impl.GlobalCommitManager;
import stm.impl.executors.ReadTransactionExecutor;
import stm.impl.executors.WriteTransactionExecutor;
import stm.impl.objectstructure.SharedObject;

import lsr.common.ClientRequest;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.service.STMService;

public class PaxosSTM {
	
	SharedObjectRegistry sharedObjectRegistry;
	private WriteTransactionExecutor writeExecutor;
	private ReadTransactionExecutor readExecutor;
	private WriteTransactionExecutor commitExecutor;
	private GlobalCommitManager globalCommitManager;
	

	private ConcurrentHashMap<RequestId, TransactionContext> requestIdContextMap;
	private ConcurrentHashMap<RequestId, Integer> requestSnapshotMap;
		
	Kryo kryo = new Kryo();
	KryoFactory factory = new KryoFactory() {
        public Kryo create () {
                        Kryo ykryo = new Kryo();
                        // configure kryo instance, customize settings
                        return ykryo;
                }
                };
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();

	private STMService service;

	public final String TX_READ_MODE = "r"; 
	public final String TX_READ_WRITE_MODE = "rw";
	public final String OBJECT_READ_MODE = "r";
	public final String OBJECT_WRITE_MODE = "w";
	
	public PaxosSTM(SharedObjectRegistry sharedObjectRegistry, int readThreadCount, int clients) {
		this.sharedObjectRegistry = sharedObjectRegistry;
		
		commitExecutor = new WriteTransactionExecutor();
		writeExecutor = new WriteTransactionExecutor(clients);
		readExecutor = new ReadTransactionExecutor(readThreadCount);
		requestIdContextMap = new ConcurrentHashMap<RequestId, TransactionContext>();
		requestSnapshotMap = new ConcurrentHashMap<RequestId, Integer>();
	}
	
	public void init(STMService service, int clientCount) {
		this.service = service;
		globalCommitManager = new GlobalCommitManager(this, service.getReplica().getPaxos(), clientCount);
		globalCommitManager.start();
	}

	/**************************************************************************
	 * Create a transaction context for requestId and store it on 
	 * requestId-context Map.
	 * @param requestId
	 */
	public void createTransactionContext(RequestId requestId, int Tid) {
		if(!requestIdContextMap.containsKey(requestId)) {
			requestIdContextMap.put(requestId, new TransactionContext(Tid));
		}
	}
	
	public void removeTransactionContext(RequestId requestId) {
		if(requestIdContextMap.containsKey(requestId)) {
			requestIdContextMap.remove(requestId);
		}
	}
	
	
	public long shutDownExecutors() {
		long failCount;
		failCount = readExecutor.shutDownWriteExecutor();
		failCount += writeExecutor.shutDownWriteExecutor();
		return failCount;
	}
	/**************************************************************************
	 * Execute the read-only transaction with multiple-thread read-executor
	 * @param request
	 */
	public void executeReadRequest(Runnable request) {
		readExecutor.execute(request);
	}

	/**************************************************************************
	 * Execute the read-write transaction with single thread write-executor
	 * @param request
	 */
	public void executeWriteRequest(Runnable request) {
		writeExecutor.execute(request);
	}
	
	public void executeCommitRequest(Runnable request) {
		commitExecutor.execute(request);
	}
	
	public void onExecuteComplete(ClientRequest request) {
		globalCommitManager.execute(request);
	}
	
	public void onCommit(RequestId rId, TransactionContext ctx) {
		service.commitBatchOnDecision(rId, ctx);
	}
	
	/**************************************************************************
	 * This method retrieves an object from the Shared object registry. It also
	 * creates the context for requestId if not created already. Finally it 
	 * adds it to read and write set according to the object access mode defined
	 * by programmer.
	 * 
	 * @param objId
	 * @param txMode
	 * @param requestId
	 * @param objectMode
	 * @return Abstractobject
	 */
	public AbstractObject open(String objId, String txMode, RequestId requestId, 
			String objectAccessMode, boolean retry, int Tid) {
		// Create the context for the request Id if it was not created before
		createTransactionContext(requestId, Tid);
		
		// Check if transaction is read-only or read-write type. For read-write add 
		// the object to writeset if object access mode says so
		if(txMode == TX_READ_MODE) { 
			if(!requestSnapshotMap.containsKey(requestId)) {				
				requestSnapshotMap.put(requestId, (Integer)sharedObjectRegistry.getSnapshot());
			}
			
			AbstractObject object = sharedObjectRegistry.getObject(requestId,objId, txMode, requestSnapshotMap.get(requestId), retry, Tid);

			// ?? Is this necessary?
//			requestIdContextMap.get(requestId).addObjectToReadSet(objId, object);
			return object;
		} else {
			// only needed to create object deep copy for write transaction
			
			// last parameter does not matter for write Tx
			int prev_owner = sharedObjectRegistry.getOwner(objId);
			AbstractObject sobject = sharedObjectRegistry.getObject(requestId,objId, txMode, 0, retry, Tid);
			
			if(sobject == null)
				return null;
			

			Kryo xkryo = pool.borrow();
                        AbstractObject object = xkryo.copy(sobject);
                        pool.release(xkryo);
		
			requestIdContextMap.get(requestId).addObjectToReadSet(objId, object);
		
			
			// for a rw transaction there may be two modes to access an object "r" or "w"
			if(objectAccessMode == OBJECT_WRITE_MODE) {
				requestIdContextMap.get(requestId).addObjectToWriteSet(objId, object);
			}
			
			if(prev_owner == Tid)
			{
			
				//System.out.println("Tx " +  Tid + " is going to re-lock Obj " + objId + " Version of object is " + object.getVersion() + "Committed version is " + sharedObjectRegistry.getLatestCommittedObject(objId).getVersion());
				// only do these ops once  
				/*requestIdContextMap.get(requestId).addObjectToReadSet(objId, object);

                        	// for a rw transaction there may be two modes to access an object "r" or "w"
                        	if(objectAccessMode == OBJECT_WRITE_MODE) {
                                	requestIdContextMap.get(requestId).addObjectToWriteSet(objId, object);
				}*/
				// increment the object version right away -- just for matching validation for read/write objects
			}
			object.incrementVersion();

			return object;
		}
	}
	
	public void storeResultToContext(RequestId requestId, byte[] result) {
		requestIdContextMap.get(requestId).setResult(result);
	}
	
	public byte[] getResultFromContext(RequestId requestId) {
		return requestIdContextMap.get(requestId).getResult();
	}
	
	public void updateUnCommittedSharedCopy(RequestId requestId) {
		// Update the non-committed but completed object copy with the 
		// Write-set of this transaction - Request ID
		TransactionContext context = requestIdContextMap.get(requestId);
		Map<String, AbstractObject> writeset = context.getWriteSet();
		for(Map.Entry<String, AbstractObject> entry: writeset.entrySet()) {
			String objId = entry.getKey();
			AbstractObject object = entry.getValue();
			sharedObjectRegistry.updateCompletedObject(objId, object);
		}
	}
	
	public boolean validateReadset(TransactionContext context) {
		assert context != null;
		if(context == null) {
			return false;
		}

		Map<String, AbstractObject> readset = context.getReadSet();
		for(Map.Entry<String, AbstractObject> entry: readset.entrySet()) {
			String objId = entry.getKey();
			AbstractObject object = entry.getValue();
			
			if(sharedObjectRegistry.getLatestCommittedObject(objId).getVersion() != (object.getVersion()-1)) {
//				System.out.print(" Validate: " + objId bjectRegistry.getLatestCommittedObject(objId).hashCode() + " ");
				//System.out.println("Failed for comparing version " + objId + " " + 
				//						sharedObjectRegistry.getLatestCommittedObject(objId).getVersion() + " != " + 
				//							(object.getVersion()-1));
				
				//sharedObjectRegistry.updateCompletedObject(objId, null);
				return false;
			}
		}
		return true;
	}

	public boolean validateAndLockReadset(TransactionContext context, int Tid) {
		
		assert context != null;
		if(context == null) {
			return false;
		}
		
		Map<String, AbstractObject> readset = context.getReadSet();
		for(Map.Entry<String, AbstractObject> entry: readset.entrySet()) {
			String objId = entry.getKey();
			AbstractObject object = entry.getValue();
			
			if(sharedObjectRegistry.getLatestCommittedObject(objId).getVersion() == (object.getVersion()-1)) 
			{
				SharedObject shObj = sharedObjectRegistry.getSharedObject(objId);
                        	boolean ret = shObj.ObjectTrylock(objId, Tid);
				
				if(!ret)
					return false;
			}
			else
			{
				return false;
			}
		}
		return true;
	}
  	/* Required to abort dummy transactions, will be removed later */

        public boolean emptyWriteSet(TransactionContext context, boolean rqueue)
        {
                //System.out.println("Going to empty the writeset");
		SharedObject shObj = null;
                if(context == null) {
                        return false;
                }

                Map<String , AbstractObject> writeset = context.getWriteSet();
                for(Map.Entry<String , AbstractObject> entry: writeset.entrySet()) {
                        String objId = entry.getKey();
                        //AbstractObject object = entry.getValue();

                        //System.out.println(" Validate: " + objId + " " + sharedObjectRegistry.getLatestCommittedObject(objId).hashCode() + " ");                              
                        //System.out.print(" Validate: " + objId + " " + sharedObjectRegistry.getLatestCommittedObject(objId).hashCode() + " ");
                	/*      System.out.println("Emptying with version " + objId + " " + 
                                                                        sharedObjectRegistry.getLatestCommittedObject(objId).getVersion() + " != " + 
                                                                                (object.getVersion()-1));*/


                /*      if((rqueue) && (abortedObjectMap.containsKey(objId)))
                        {
        
                                continue;
                        }
                        else
                        {
                                 sharedObjectRegistry.updateCompletedObject(objId, null);
                        }*/
			
			shObj = sharedObjectRegistry.getSharedObject(objId);
			//shObj.ObjectUnlock(objId, requestId);
                        
			//System.out.println("Abort UnLocking Object " + objId);
			//sharedObjectRegistry.updateCompletedObject(objId, null);
                	//System.out.println("After setting to null, ownerof object " + objId + " is " + sharedObjectRegistry.getOwner(objId));
                        //abortedObjectMap.put(objId,new AbortEntry(sharedObjectRegistry.getLatestCommittedObject(objId).getVersion()));
                }

                return true;
        }

        public boolean lockReadSet(TransactionContext context, RequestId requestId, int Tid)
        {
                //System.out.println("Going to unlock the readset");
		SharedObject shObj = null;
                if(context == null) {
                        return false;
                }

                Map<String , AbstractObject> readset = context.getReadSet();
                for(Map.Entry<String , AbstractObject> entry: readset.entrySet()) {
                        String objId = entry.getKey();
			
			shObj = sharedObjectRegistry.getSharedObject(objId);
			boolean ret = shObj.ObjectTrylock(objId, Tid);
                        if(!ret)
				return false;
                }
		return true;
        }


        public void unlockReadSet(TransactionContext context, RequestId requestId, int Tid)
        {
                //System.out.println("Going to unlock the readset");
		SharedObject shObj = null;
                if(context == null) {
                        return;
                }

                Map<String , AbstractObject> readset = context.getReadSet();
                for(Map.Entry<String , AbstractObject> entry: readset.entrySet()) {
                        String objId = entry.getKey();
			
			shObj = sharedObjectRegistry.getSharedObject(objId);
			shObj.ObjectUnlock(objId, requestId, Tid);
                        
			//System.out.println("Abort UnLocking Object " + objId);
                }

        }

	public boolean updateSharedObject(TransactionContext context, RequestId requestId, int Tid) {
		boolean commit = true;
	
		long client = requestId.getClientId();
		long SeqNumber = requestId.getSeqNumber();	
		//System.out.println("Calling updateSharedObject for client " + client + " SeqNumber " + SeqNumber + " Tid = " + Tid);
		Map<String, AbstractObject> writeset = context.getWriteSet();
		Map<String, AbstractObject> readset = context.getReadSet();
		SharedObject shObj = null;
		int timeStamp = sharedObjectRegistry.getNextSnapshot();
		// Update all shared objects with shadowcopy object values and versions 
		// Acquire lock over all objects - for multithreaded STM
		
		for(Map.Entry<String, AbstractObject> entry: writeset.entrySet()) {
			// update all objects
			String objId = entry.getKey();
			sharedObjectRegistry.updateObject(entry.getKey(), entry.getValue(), timeStamp);
			shObj = sharedObjectRegistry.getSharedObject(objId);
			readset.remove(objId);
			//System.out.println("Tx " + Tid + " Going to unlock writeset " + objId);
			shObj.ObjectUnlock(objId, requestId, Tid);
			//System.out.println("Commit UnLocking Writeset Object " + objId);	
		}
		// release lock from the remaining readset objects
		for(Map.Entry<String, AbstractObject> entry: readset.entrySet()) {
			// update all objects
			String objId = entry.getKey();
			shObj = sharedObjectRegistry.getSharedObject(objId);
			//System.out.println("Tx = " + Tid + " Going to unlock readset " + objId);
			shObj.ObjectUnlock(objId, requestId, Tid);
			//System.out.println("Commit UnLocking Readset Object " + objId);	
		}
		//System.out.println("Exit updateSharedObject");

		
		return commit;
	}
	
	public TransactionContext getTransactionContext(RequestId requestId) {
		return requestIdContextMap.get(requestId);
	}

	public void notifyCommitManager(Request request) {
		globalCommitManager.notify(request);		
	}

	public STMService getSTMService() {
		return service;
	}
	
}
