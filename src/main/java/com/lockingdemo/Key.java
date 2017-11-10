package com.lockingdemo;

import java.util.concurrent.ExecutionException;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.Response.Header;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;

public class Key {
	public static Header waitDeletes(Client client, String prefix, long maxCreateRev) {
		KV kv = client.getKVClient();
		GetResponse getResponse = null;

		while (true) {
			try {
			ByteSequence prefixBytes = new ByteSequence(prefix);
				getResponse = kv.get(prefixBytes, GetOption.newBuilder()
						.withPrefix(prefixBytes)
						.withSortField(GetOption.SortTarget.CREATE)
						.withSortOrder(SortOrder.DESCEND).build()).get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if (getResponse.getCount() == 0 ||
					(getResponse.getCount() == 1 && 
					getResponse.getKvs().get(0).getCreateRevision() > maxCreateRev)) {
				return getResponse.getHeader();
			}

			int idx = 0;
			if (getResponse.getKvs().get(0).getCreateRevision() > maxCreateRev) {
				idx++;
			}
			ByteSequence key = getResponse.getKvs().get(idx).getKey();
			
			waitDelete(client, key, getResponse.getKvs().get(idx).getCreateRevision());
		}
	}

	private static void waitDelete(Client client, ByteSequence key, long revision) {
		Watch watchClient = client.getWatchClient();
		Watcher watch = watchClient.watch(key, WatchOption.newBuilder().withRevision(revision).build());

		try {
			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				WatchResponse watchResponse = watch.listen();
				boolean deleted = watchResponse.getEvents().stream().anyMatch(e -> {
					return e.getEventType().equals(WatchEvent.EventType.DELETE);
				});
				if (deleted) {
					break;
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
