package stm.benchmark.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import stm.benchmark.bank.BankMultiClient;
import stm.impl.PaxosSTM;
import stm.impl.SharedObjectRegistry;
import lsr.common.Configuration;
import lsr.paxos.ReplicationException;
import lsr.paxos.replica.Replica;

public class TpccServer {

	public static void main(String[] args) throws IOException,
			InterruptedException, ExecutionException, ReplicationException {
		if (args.length < 8 || args.length > 8) {
			usage();
			System.exit(1);
		}
		int localId = Integer.parseInt(args[0]);
		int warehouseCount = Integer.parseInt(args[1]); // Default is 1000
		int itemCount = Integer.parseInt(args[2]);
		int readThreadCount = Integer.parseInt(args[3]);
		int readPercentage = Integer.parseInt(args[4]);
		boolean tpccProfile = Integer.parseInt(args[5]) == 0 ? false : true;
		int clientCount = Integer.parseInt(args[6]);
		int requests = Integer.parseInt(args[7]);

		//System.out.println("Id = "+ localId + " readthreadCount = " + readThreadCount );
		Configuration process = new Configuration();
		 //System.out.println("New Tpcc");

		Tpcc tpcc = new Tpcc();
		
		//System.out.println("New SharedObject");
		SharedObjectRegistry sharedObjectRegistry = new SharedObjectRegistry(
				itemCount);
		 //System.out.println("Init PaxosSTM");
		PaxosSTM stmInstance = new PaxosSTM(sharedObjectRegistry,
				readThreadCount, clientCount);
		//System.out.println("TpccInit");
		tpcc.TpccInit(sharedObjectRegistry, stmInstance, warehouseCount,
				itemCount);
		//System.out.println("Replica Init");
		Replica replica = new Replica(process, localId, tpcc);
		tpcc.setReplica(replica);

		TpccMultiClient client = new TpccMultiClient(clientCount, requests,
				readPercentage, tpccProfile, tpcc);

		replica.start();

		tpcc.initRequests();

		stmInstance.init(tpcc, clientCount);

		client.run();

		System.in.read();

//		System.in.read();
//		System.exit(-1);
	}

	private static void usage() {
		System.out
				.println("Invalid arguments. Usage:\n"
						+ "   java lsr.paxos.Replica <replicaID> <Number Of Warehouses - 20> <Number Of Items - 5000> <Number of Read-Threads> "
						+ "<readPercentage> <tpccProfile> <clientCount> <#reqs>");
	}
}
