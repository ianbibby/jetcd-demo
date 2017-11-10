package com.lockingdemo;

import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@SpringBootApplication
public class SpringLockingDemoApplication implements ApplicationRunner {
	
	private List<String> servers;
	private List<String> locks;
	private long duration; // unit: seconds, default: 5

	public static void main(String[] args) {
		SpringApplication.run(SpringLockingDemoApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) {
		try {
			servers = parseServers(args);
			locks = parseLocks(args);
			duration = parseDuration(args);
			
			dispatchJob(locks);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void dispatchJob(List<String> locks) throws Exception {
		Client client = Client.builder().endpoints(servers).build();

		long leaseID = createLease(client, 10);
		// Lock
		List<Mutex> mutexes = Lists.newArrayList();
		locks.forEach(lock -> {
			Mutex mutex = new Mutex(client, leaseID, "/" + lock);
			mutex.lock();
		});

		System.out.println("Doing some long running task...");
		Thread.sleep(duration * 1000);
		
		// Unlock in reverse order
		ListIterator<Mutex> iterator = mutexes.listIterator(mutexes.size());
		while (iterator.hasPrevious()) {
			iterator.previous().unlock();
		}
		
		System.out.println("Done");

		client.getLeaseClient().revoke(leaseID);
		client.close();
		System.exit(0);
	}

	/**
	 * Creates a lease with a given TTL.
	 * 
	 * @param client
	 *            etcd client
	 * @param ttl
	 *            ttl in seconds
	 * @return lease ID
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private long createLease(Client client, long ttl) throws InterruptedException, ExecutionException {
		Lease leaseClient = client.getLeaseClient();
		LeaseGrantResponse leaseResp = leaseClient.grant(120).get();
		return leaseResp.getID();
	}
	
	private List<String> parseServers(ApplicationArguments args) {
		List<String> values = args.getOptionValues("servers");
		if (values == null || values.isEmpty()) {
			return Lists.newArrayList("http://localhost:2379");
		}
		
		Set<String> servers = Sets.newHashSet();
		values.forEach(val -> {
			String[] split = val.split(",");
			servers.addAll(Sets.newHashSet(split));
		});
		
		List<String> sortedServers = Lists.newArrayList(servers);
		Collections.sort(sortedServers);
		return sortedServers;
	}
	
	private List<String> parseLocks(ApplicationArguments args) {
		List<String> lockValues = args.getOptionValues("locks");
		if (lockValues == null || lockValues.isEmpty()) {
			throw new IllegalArgumentException("Must specify one or more locks.");
		}
		
		Set<String> locks = Sets.newHashSet();
		lockValues.forEach(val -> {
			String[] split = val.split(",");
			locks.addAll(Sets.newHashSet(split));
		});
		
		List<String> sortedLocks = Lists.newArrayList(locks);
		Collections.sort(sortedLocks);
		return sortedLocks;
	}
	
	private long parseDuration(ApplicationArguments args) {
		List<String> values = args.getOptionValues("duration");
		if (values == null || values.isEmpty()) {
			return 5;
		}
		try {
			return Long.parseLong(values.get(0));
		} catch (NumberFormatException nfe) {
			throw new IllegalArgumentException("Invalid duration: " + values.get(0));
		}
	}

}
