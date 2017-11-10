package com.lockingdemo;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Txn;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.data.Response.Header;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.TxnResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.op.Op.GetOp;
import com.coreos.jetcd.op.Op.PutOp;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.options.GetOption.SortOrder;

public class Mutex {
	
	private Client client;
	private long leaseID;
	private String prefix;
	private Header header;
	
	public Mutex(Client client, long leaseID, String prefix) {
		this.client = client;
		this.leaseID = leaseID;
		this.prefix = prefix;
	}
	
	public void lock() {
		KV kvClient = client.getKVClient();
		Cmp cmp = new Cmp(getKey(), Cmp.Op.EQUAL, CmpTarget.createRevision(0));
		
		// put self in lock waiters via key; oldest waiter holds lock
		PutOp put = Op.put(getKey(), new ByteSequence(""), PutOption.newBuilder().withLeaseId(leaseID).build());
		// Reuse key in case this session already holds the lock
		GetOp get = Op.get(getKey(), GetOption.DEFAULT);
		// Fetch current holder to complete uncontended path with only one RPC
		ByteSequence prefixBytes = new ByteSequence(prefix);
		GetOp getOwner = Op.get(prefixBytes, GetOption.newBuilder()
				.withPrefix(prefixBytes)
				.withSortField(GetOption.SortTarget.CREATE)
				.withSortOrder(SortOrder.ASCEND)
				.withLimit(1).build());
		
		Txn txn = kvClient.txn();
		TxnResponse resp = null;
		try {
			resp = txn.If(cmp).Then(put, getOwner).Else(get, getOwner).commit().get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return;
		}
		
		long myRevision = resp.getHeader().getRevision();
		if (!resp.isSucceeded()) {
			myRevision = resp.getGetResponses().get(0).getKvs().get(0).getCreateRevision();
		}
		
		// Get current owner
		List<KeyValue> ownerKey = resp.getGetResponses().get(0).getKvs();
		try {
			GetResponse existing = kvClient.get(prefixBytes, GetOption.newBuilder()
					.withPrefix(prefixBytes)
					.withSortField(GetOption.SortTarget.CREATE)
					.withSortOrder(SortOrder.ASCEND)
					.withLimit(1).build()).get();
			ownerKey = existing.getKvs();
			existing.getKvs().forEach(k -> {
				System.out.println("Existing: " + k.getKey().toStringUtf8() + ", Rev=" + k.getCreateRevision());
				
			});
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		long ownerRevision = ownerKey.get(0).getCreateRevision();
		if (ownerKey.size() == 0 || ownerRevision == myRevision) {
			header = resp.getHeader();
			return;
		}
		
		System.out.println("Waiting for deletion of revisions <= " + (myRevision-1) );
		this.header = Key.waitDeletes(client, prefix, (myRevision-1));
	}
	
	public void unlock() throws InterruptedException, ExecutionException {
		KV kvClient = client.getKVClient();
		kvClient.delete(getKey()).get();
		leaseID = 0;
		prefix = null;
	}
	
	private ByteSequence getKey() {
		return new ByteSequence(String.format("%s/%x", prefix, leaseID));
	}
}
