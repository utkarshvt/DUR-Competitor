package stm.benchmark.bank;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.esotericsoftware.kryo.Kryo;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.replica.Replica;
import lsr.paxos.replica.SnapshotListener;
import lsr.service.STMService;
import stm.impl.PaxosSTM;
import stm.impl.SharedObjectRegistry;
import stm.transaction.AbstractObject;
import stm.transaction.TransactionContext;
import stm.benchmark.bank.Account;

/*****************************************************************************
 * 
 * @author sachin This is the Bank class which implements a well known monetary
 *         application used in Transaction Memory benchmarks. It extends the STM
 *         Service which is needed for connection to SPaxos.
 ****************************************************************************/
public class Bank extends STMService {
	protected final int DEFAULT_NUM_ACCOUNTS = 256;
	protected final int INITIAL_BALANCE = 1000;
	protected final int DEFAULT_TRANSACTION_AMOUNT = 10;
	// protected final int RETRY_COUNT = 10; // It should not even be more than
	// 1, but still

	public final byte TX_TRANSFER = 1;
	public final byte TX_GETBALANCE = 2;
	public final int DEFAULT_LENGTH = 10;

	protected int numAccounts;
	SharedObjectRegistry sharedObjectRegistry;
	PaxosSTM stmInstance;
	Replica replica;
	BankMultiClient client;

	/** Main Thread to issue read/write/commit requests */
	private SingleThreadDispatcher bankSTMDispatcher;

	String ACCOUNT_PREFIX = "account_";
	Random random = new Random();

	private long startRead;
	private long endRead;
	private long lastReadCount = 0;
	private long lastWriteCount = 0;
	private long lastAbortCount = 0;
 	private long lastCompletedCount = 0;
	private long lastearlyAbort = 0;


	private volatile long readCount = 0;
	/**
	 * Store request content w.r.t. requestId. It is useful when tansaction is
	 * aborted and retried.
	 */
	private final Map<RequestId, byte[]> requestIdValueMap = new HashMap<RequestId, byte[]>();

	private volatile long completedCount = 0;
	private volatile long committedCount = 0;
	private volatile long abortedCount = 0;
	private volatile long earlyAbort = 0;

	private int localId;
	private int min;
	private int max;
	private int numReplicas;
	private int accessibleObjects;

	private AtomicInteger TransactionId;
	/**
	 * Temporary storage for the batches that were received (may be out of
	 * order).
	 */
	// private final Map<ClientBatchID, ClientRequest[]> batchesWaitingExecution
	// =
	// new HashMap<ClientBatchID, ClientRequest[]>();

	MonitorThread monitorTh = new MonitorThread();

	/*************************************************************************
	 * This class is only for taking the readings from the experiment. The
	 * sampling thread is triggered when read/write count reaches a particular
	 * limit, it goes to sleep for 20 seconds and then it samples the reading.
	 * 
	 * @author sachin
	 * 
	 ************************************************************************/
	class MonitorThread extends Thread {
		public void run() {
			long count = 0;
			long localReadCount = 0;
			long localWriteCount = 0;
			long localAbortCount = 0;
			long localCompletedCount = 0;
                        long localearlyAbort = 0;

			System.out
					.println("Read-Throughput/S  Write Throughput/S  CompletdCount  Write Latency  Abort  LocalAborts  Time");
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			System.out.println("");
			// Sample time is 20 seconds
			while (count < 10) {
				startRead = System.currentTimeMillis();

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				localReadCount = readCount;
				localWriteCount = committedCount;
				localAbortCount = abortedCount + 1;

				localCompletedCount = completedCount;
                                localearlyAbort = earlyAbort;

				endRead = System.currentTimeMillis();
				client.collectLatencyData();

				System.out
						.format("%6d  %6d   %6d   %5.2f   %6d   %6d   %6d\n",
								((localReadCount - lastReadCount) * 1000)
										/ (endRead - startRead),
								((localWriteCount - lastWriteCount) * 1000)
										/ (endRead - startRead),
								((localCompletedCount - lastCompletedCount) * 1000)
										/ (endRead - startRead),
								client.getWriteLatency(),
								((localAbortCount - lastAbortCount) * 1000)
										/ (endRead - startRead),
								((localearlyAbort - lastearlyAbort) * 1000)
										/ (endRead - startRead),
								//((localAbortCount - lastAbortCount) * 100 / ((localAbortCount - lastAbortCount) + (localWriteCount - lastWriteCount))),
								(endRead - startRead));

				lastReadCount = localReadCount;
				lastWriteCount = localWriteCount;
				lastAbortCount = localAbortCount;
				lastCompletedCount = localCompletedCount;
				lastearlyAbort = localearlyAbort;

				count++;
			}
		
			try
                        {
                                Thread.sleep(10);
                        } catch (InterruptedException e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                        }

                        System.exit(0);

		}
	
		
	}

	/*************************************************************************
	 * Prints the completed v/s committed transactions after the monitor thread
	 * wakes up.
	 ************************************************************************/
	public void printServerStats() {
		System.out.println("Completed Tx = " + completedCount
				+ ", Committed Tx = " + committedCount);
	}

	/*************************************************************************
	 * This method initializes the class variables and creates instances of
	 * Account objects and registers them to ShareddObjectRegistry.
	 * 
	 * @param numAccounts
	 * @param sharedObjectRegistry
	 * @param stminstance
	 * 
	 ************************************************************************/
	public void init(int numAccounts,
			SharedObjectRegistry sharedObjectRegistry, PaxosSTM stminstance) {
		this.sharedObjectRegistry = sharedObjectRegistry;
		this.numAccounts = numAccounts;
		for (int i = 0; i < this.numAccounts; i++) {
			String accountId = ACCOUNT_PREFIX + Integer.toString(i);
			Account account = new Account(INITIAL_BALANCE, accountId);
			this.sharedObjectRegistry.registerObjects(accountId, account);
		}

		this.stmInstance = stminstance;
		this.bankSTMDispatcher = new SingleThreadDispatcher("BankSTM");
		// this.bankSTMDispatcher.start();
		monitorTh.start();
	}

	public void initRequests() {
		this.localId = ProcessDescriptor.getInstance().localId;
		this.numReplicas = ProcessDescriptor.getInstance().numReplicas;
		this.TransactionId = new AtomicInteger(this.localId + 1);
		this.accessibleObjects = numAccounts / numReplicas;
		this.min = this.accessibleObjects * this.localId;
		this.max = (this.accessibleObjects * (this.localId + 1)) - 1;
		// System.out.println("O:" + this.accessibleObjects + "M:" + this.max +
		// "m:" + this.min);
	}

	/*************************************************************************
	 * This is the speculative execution implementation for getBalance method.
	 * It gets the object from committed version if there is no completed
	 * version in the system, otherwise picks up from the completed version of
	 * last transaction which updated the concerned object's completed version.
	 * After the operations are finished on objects, result of transaction is
	 * stored in transaction context.
	 * 
	 * @param src
	 * @param dst
	 * @param requestId
	 * 
	 ************************************************************************/
	public void getBalance(ClientRequest cRequest, int src, int dst,
			boolean retry, int Tid) {
		// Multiversion - Take object copy according to Tx type

		Account srcAccount, dstAccount;
		String srcId = ACCOUNT_PREFIX + Integer.toString(src);
		String dstId = ACCOUNT_PREFIX + Integer.toString(dst);

		RequestId requestId = cRequest.getRequestId();
		srcAccount = (Account) stmInstance.open(srcId, "r", requestId, "r",
				retry,0);
		dstAccount = (Account) stmInstance.open(dstId, "r", requestId, "r",
				retry,0);

		int balance = srcAccount.getAmount() + dstAccount.getAmount();

		// Send the balance to requesting client
		sendReply(ByteBuffer.allocate(4).putInt(balance).array(), cRequest);

		readCount++;

		return; // balance;
	}

	/*************************************************************************
	 * This is the optimistic method implementation for transfer method. It gets
	 * the object from stable (in-memory) storage if there is no shadow copy in
	 * the system, otherwise picks up from the shadow copy of last transaction
	 * which updated the concerned object in its shadow copy. After the
	 * operations are finished on objects, status of objects is stored in
	 * transaction context.
	 * 
	 * @param src
	 * @param dst
	 * @param requestId
	 * 
	 ************************************************************************/
	public void transfer(ClientRequest cRequest, int src, int dst, boolean retry, int Tid) {

		Integer success = 0;
		String srcId = ACCOUNT_PREFIX + Integer.toString(src);
		String dstId = ACCOUNT_PREFIX + Integer.toString(dst);

		boolean xretry = true;
		// Multi version - Take object copy from completed but not committed if
		// present
		Account srcAccount, dstAccount;

		RequestId requestId = cRequest.getRequestId();
		
		while(xretry == true)
		{
			xretry = false;
			srcAccount = (Account) stmInstance.open(srcId, "rw", requestId, "w",
					retry, Tid);
			dstAccount = (Account) stmInstance.open(dstId, "rw", requestId, "w",
					retry, Tid);

			// Operation performed over the objects
			srcAccount.withdraw(DEFAULT_TRANSACTION_AMOUNT); // Modify the
															// shadowcopy
			dstAccount.deposit(DEFAULT_TRANSACTION_AMOUNT); // Modify the shadowcopy

			// update shared copy completed-but-not-committed copy with the write
			// set

			TransactionContext ctx = stmInstance.getTransactionContext(requestId);
                    	if(!stmInstance.validateAndLockReadset(ctx, Tid))
                        {
                                xretry = true;
                                stmInstance.unlockReadSet(ctx, requestId, Tid);
                                earlyAbort++;
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
		//stmInstance.updateUnCommittedSharedCopy(requestId);
		completedCount++;

		byte[] result = ByteBuffer.allocate(4).putInt(success).array();
		stmInstance.storeResultToContext(requestId, result);
	}

	/**
	 * Used by createRequest when a request is created by client and src and dst
	 * account numbers are randomly generated based on this field.
	 * 
	 * @param client
	 */
	public void initClient(int numAccounts, BankMultiClient client) {
		this.numAccounts = numAccounts;
		this.client = client;
	}

	/**
	 * Pass reference to replice for sending a reply to client after read
	 * request is executed or write request is committed.
	 */
	public void setReplica(Replica replica) {
		this.replica = replica;
	}

	/**
	 * Used to execute read requests from clients locally.
	 */
	@Override
	public void executeReadRequest(final ClientRequest cRequest) {
		// TODO Auto-generated method stub
		bankSTMDispatcher.submit(new Runnable() {
			public void run() {

				executeRequest(cRequest, false);
			}
		});
	}

	/**
	 * This method is used for three purposes. 1. For read transaction 2. For
	 * write transaction which is executed speculatively 3. For write
	 * transaction which is retied after commit failed
	 * 
	 * @param request
	 * @param retry
	 */
	public void executeRequest(final ClientRequest request, final boolean retry) {
		byte[] value = request.getValue();
		ByteBuffer buffer = ByteBuffer.wrap(value);
		final RequestId requestId = request.getRequestId();

		byte transactionType = buffer.get();
		byte command = buffer.get();
		final int src = buffer.getInt();
		final int dst = buffer.getInt();
		final int Tid = TransactionId.getAndAdd(this.numReplicas);


		if (transactionType == READ_ONLY_TX) {
			if (command == TX_GETBALANCE) {
				// Keep the request stored locally to retry in case version
				// match fails - not needed for read Tx
				// requestIdValueMap.put(requestId, request);
				stmInstance.executeReadRequest(new Runnable() {
					public void run() {
						getBalance(request, src, dst, retry, Tid);
					}
				});
			} else {
				System.out.println("Wrong RD command " + command
						+ " transaction type " + transactionType);
			}
		} else {
			if (command == TX_TRANSFER) {
				// Keep the request stored locally to retry in case version
				// match fails
				if (retry == false) {
					requestIdValueMap.put(requestId, value);
					// assert stmInstance != null;
					stmInstance.executeWriteRequest(new Runnable() {
						public void run() {
							// retry boolean flag is false for first time
							// execution
							transfer(request, src, dst, retry, Tid);
							// System.out.print("&");
							stmInstance.onExecuteComplete(request);
						}
					});
				} else {
					// Yet to implement... Balaji
					// transfer(request, src, dst, retry);
				}
			} else {
				System.out.println("Wrong WR command " + command
						+ " transaction type " + transactionType);
			}
		}
	}

	/**
	 * A common interface to send the reply to client request back to client
	 * through replica
	 * 
	 * @param result
	 * @param cRequest
	 */
	public void sendReply(byte[] result, ClientRequest cRequest) {
		// replica.replyToClient(result, cRequest);
	}

	@Override
	public void notifyCommitManager(Request request) {
		// System.out.print("!");
		stmInstance.notifyCommitManager(request);
	}

	/**
	 * Called by network layer to commit a previous speculatively executed
	 * batch.
	 */
	@Override
	public void commitBatchOnDecision(final RequestId rId,
			final TransactionContext txContext) {
		// TODO Auto-generated method stub
		// Validate sequence
		// If validated - commit -- Delete RequestId from
		// LocalTransactionManager.requestDirtycopyMap
		// else abort and retry

		stmInstance.executeCommitRequest(new Runnable() {
			public void run() {
				onCommit(rId, txContext);
			}
		});
	}

	/*************************************************************************
	 * This method commits the given transaction. It first validates the readset
	 * and then after validating shadowcopy too, updates the objects in
	 * SharedObjectRegistry with the shadowcopy. Finally it removes all the data
	 * for optimistically executed transaction (cleanup).
	 * 
	 * @param requestId
	 * @param commandType
	 * @return
	 * 
	 ************************************************************************/
	public void onCommit(RequestId requestId, TransactionContext txContext) {

		// Validate the transaction object versions or Decided InstanceIds and
		// sequence numbers
		// Check the version of object in stable copy and see if the current
		// shadowcopy version is +1
		// and InstanceId of shadowcopy matches with the stable copy.
		// Object object = null;
		// boolean retry = true;
		// RequestId requestId = cRequest.getRequestId();

		// Validate read set first
                int Tid = 0;
                TransactionContext old_ctx = stmInstance.getTransactionContext(requestId);
                if(old_ctx != null)
                {
                        Tid = old_ctx.getTransactionId();

                }
		


		if (stmInstance.validateReadset(txContext)) {
			stmInstance.updateSharedObject(txContext, requestId, Tid);
			committedCount++;
			// break;
		} else {
			// Isn;t it needed to remove the previous content

			stmInstance.unlockReadSet(txContext, requestId, Tid);
                        stmInstance.removeTransactionContext(requestId);

                        byte[] value = requestIdValueMap.get(requestId);
                        if(value != null)
                        {
                                ClientRequest cRequest = new ClientRequest(requestId , value);

                                executeRequest(cRequest, false);
                        }

			abortedCount++;
			return;
			// executeRequest(cRequest, retry);
			// stmInstance.updateSharedObject(requestId);
		}
		// remove the entries for this transaction LTM (TransactionContext,
		// lastModifier)
		// object = stmInstance.getResultFromContext(requestId);
		// sendReply(stmInstance.getResultFromContext(requestId), cRequest);
		client.replyToClient(requestId);

		stmInstance.removeTransactionContext(requestId);
		requestIdValueMap.remove(requestId);
	}

	/**
	 * Read the command name from the client request byte array.
	 * 
	 * @param value
	 * @return
	 */
	public byte getCommandName(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		buffer.get();
		byte command = buffer.get();
		buffer.flip();
		return command;

	}

	/************************************************************************
	 * 
	 * @param requestId
	 * 
	 *            This method rollsback all the changes performed on shadowcopy
	 *            of the optimistically executed transaction.
	 */
	public void rollback(RequestId requestId) {
		stmInstance.removeTransactionContext(requestId);
	}

	/************************************************************************
	 * 
	 * @return
	 * 
	 *         This method is used to find Total Balance in all accounts
	 *         registered with Bank instance. It is used to carry out sanity
	 *         check.
	 ************************************************************************/
	public boolean checkBalances() {
		int sum = 0;
		for (int i = 0; i < sharedObjectRegistry.getCapacity(); i++) {
			Account account = (Account) sharedObjectRegistry
					.getLatestCommittedObject(ACCOUNT_PREFIX
							+ Integer.toString(i));
			sum += account.getAmount();
			System.out.println("Account[" + i + "] = " + account.getAmount());
		}

		if (sum != (INITIAL_BALANCE * numAccounts)) {
			System.out
					.printf("The sumBalances returned a value (%d) different than it should (%d)!\n",
							sum, (INITIAL_BALANCE * numAccounts));
			return false;
		}
		return true;
	}

	/************************************************************************
	 * This method performs sanity check on all registered objects.
	 ***********************************************************************/
	public void sanityCheck() {
		if (checkBalances()) {
			System.out.println("Sanity Check passed !!!");
		}
	}
	
	@Override
	public byte[] serializeTransactionContext(TransactionContext ctx) {
		Map<String, AbstractObject> readset = ctx.getReadSet();
		Map<String, AbstractObject> writeset = ctx.getWriteSet();

		int packetSize = 4 + (readset.size() * 12) + 4
				+ (writeset.size() * 16);
		ByteBuffer bb = ByteBuffer.allocate(4 + packetSize);

		bb.putInt(4 + packetSize);
		bb.putInt(readset.size());
		for (Map.Entry<String, AbstractObject> entry : readset.entrySet()) {
			String id = entry.getKey();
			id = id.replace("account_", "");

			bb.putInt(Integer.parseInt(id));
			bb.putLong(entry.getValue().getVersion());
		}

		bb.putInt(writeset.size());
		for (Map.Entry<String, AbstractObject> entry : writeset.entrySet()) {
			String id = entry.getKey();
			id = id.replace("account_", "");

			Account account = (Account) entry.getValue();
			bb.putInt(Integer.parseInt(id));
			bb.putLong(account.getVersion());
			bb.putInt(account.getAmount());
		}

		bb.flip();
		return bb.array();
	}
	
	@Override
	public TransactionContext deserializeTransactionContext(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		assert bytes.length == bb.getInt() : "bank byte deserializing error";

		TransactionContext ctx = new TransactionContext();

		int readsetSize = bb.getInt();
		for (int i = 0; i < readsetSize; i++) {
			String id = "account_" + bb.getInt();
			long version = bb.getLong();
			Account account = new Account();
			account.setId(id);
			account.setVersion(version);

			ctx.addObjectToReadSet(id, account);
		}

		int writesetSize = bb.getInt();
		for (int i = 0; i < writesetSize; i++) {
			String id = "account_" + bb.getInt();
			long version = bb.getLong();
			int value = bb.getInt();

			Account account = new Account();
			account.setId(id);
			account.setVersion(version);
			account.setAmount(value);

			ctx.addObjectToWriteSet(id, account);
		}

		return ctx;
	}

	/**
	 * This method fills the parameters in the request byte array for client for
	 * the bank benchmark.
	 * 
	 * @param request
	 *            : byte array
	 * @param readOnly
	 *            : boolean specifying what should be the transaction type
	 */
	public byte[] createRequest(boolean readOnly) {
		byte[] request = new byte[DEFAULT_LENGTH];

		ByteBuffer buffer = ByteBuffer.wrap(request);
		if (readOnly) {
			buffer.put(READ_ONLY_TX);
			buffer.put(TX_GETBALANCE);
		} else {
			buffer.put(READ_WRITE_TX);
			buffer.put(TX_TRANSFER);
		}
		int src = random.nextInt(max - min) + min;
		int dst = random.nextInt(max - min) + min;
		while (src == dst) {
			dst = random.nextInt(max - min) + min;
		}
		buffer.putInt(src);
		buffer.putInt(dst);

		buffer.flip();
		return request;
	}

	/**
	 * Shuts down the executors if invoked. Here after no transaction can be
	 * performed.
	 * 
	 * @return
	 */
	public long shutDownExecutors() {
		return stmInstance.shutDownExecutors();
	}

	@Override
	public Replica getReplica() {
		return this.replica;
	}

	public void askForSnapshot(int lastSnapshotInstance) {
		// ignore
	}

	public void forceSnapshot(int lastSnapshotInstance) {
		// ignore
	}

	public void updateToSnapshot(int instanceId, byte[] snapshot) {
		// ignore
	}

	// @Override
	public byte[] execute(byte[] value, int executeSeqNo) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addSnapshotListener(SnapshotListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeSnapshotListener(SnapshotListener listener) {
		// TODO Auto-generated method stub

	}

}
