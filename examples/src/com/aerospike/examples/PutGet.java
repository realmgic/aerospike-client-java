/*
 * Copyright 2012-2023 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.examples;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PutGet extends Example {

	public PutGet(Console console) {
		super(console);
	}

	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		for (int i = 0; i < 200; i++) {
			try {
				console.info("---------------------------------------------------------------------------------------");
				runBatch(client, params);
			} catch (Exception E) {
				console.warn("exception: " + E);
				throw E;
			}

			TimeUnit.SECONDS.sleep(1);
		}
	}

	/**
	 * Execute put and get on a server configured as multi-bin.  This is the server default.
	 */
	private void runMultiBinTest(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "putgetkey");
		Bin bin1 = new Bin("bin1", "value1");
		Bin bin2 = new Bin("bin2", "value2");

		console.info("Put: namespace=%s set=%s key=%s bin1=%s value1=%s bin2=%s value2=%s",
				key.namespace, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);

		client.put(params.writePolicy, key, bin1, bin2);

		console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);
		Policy p = params.policy;
		p.replica = Replica.SEQUENCE;
		p.maxRetries = 0;
		p.totalTimeout = 1000;

		Record record = client.get(p, key);

		if (record == null) {
			throw new Exception(String.format(
					"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
		}
//
//		validateBin(key, bin1, record);
//		validateBin(key, bin2, record);

	}

	private void runBatch(IAerospikeClient client, Parameters params) throws Exception {
		List<BatchRead> records = new ArrayList<BatchRead>();

		records.add(new BatchRead(new Key(params.namespace, params.set, "putgetkey1"),true));
		records.add(new BatchRead(new Key(params.namespace, params.set, "putgetkey2"),true));
		records.add(new BatchRead(new Key(params.namespace, params.set, "putgetkey3"),true));
		records.add(new BatchRead(new Key(params.namespace, params.set, "putgetkey4"),true));
		records.add(new BatchRead(new Key(params.namespace, params.set, "putgetkey5"),true));
		records.add(new BatchRead(new Key(params.namespace, params.set, "putgetkey6"),true));

		BatchPolicy pB = client.getBatchPolicyDefault();
		pB.totalTimeout = 1000;
		pB.socketTimeout = 100;
		pB.maxRetries = 3;
		pB.maxConcurrentThreads = 0;
		pB.replica = Replica.SEQUENCE;
		// pB.connectTimeout = 10000;
/*
    batchPolicy.maxConcurrentThreads = 0;
    batchPolicy.socketTimeout = 100;
    batchPolicy.maxRetries = 3;
    batchPolicy.totalTimeout = 1000;
    batchPolicy.replica = Replica.SEQUENCE;
 */
		console.info("before get");
		client.get(pB, records);

		console.info("after  get");

		String binName = "bin1";

		for (BatchRead record : records) {
			Key key = record.key;
			Record rec = record.record;

			console.info("res: " + record.resultCode);
			if (rec != null) {
				console.info("Record: ns=%s set=%s key=%s bin=%s value=%s",
						key.namespace, key.setName, key.userKey, binName, rec.getValue(binName));
			}
			else {
				console.info("Record not found: ns=%s set=%s key=%s bin=%s",
						key.namespace, key.setName, key.userKey, binName);
			}
		}
	}

	/**
	 * Execute put and get on a server configured as single-bin.
	 */
	private void runSingleBinTest(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "putgetkey");
		Bin bin = new Bin("b", "value");

		console.info("Single Bin Put: namespace=%s set=%s key=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.value);

		client.put(params.writePolicy, key, bin);

		console.info("Single Bin Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

		Record record = client.get(params.policy, key, "b");

		if (record == null) {
			throw new Exception(String.format(
					"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
		}

		validateBin(key, bin, record);
	}

	private void validateBin(Key key, Bin bin, Record record) {
//		Object received = record.getValue(bin.name);
		Object received = record.getValue("bbb");

		String expected = bin.value.toString();

		if (received != null && received.equals(expected)) {
			console.info("Bin matched: namespace=%s set=%s key=%s bin=%s value=%s generation=%d expiration=%d",
					key.namespace, key.setName, key.userKey, bin.name, received, record.generation, record.expiration);
		}
		else {
			console.error("Put/Get mismatch: Expected %s. Received %s.", expected, received);
		}
	}

	/**
	 * Read record header data.
	 */
	private void runGetHeaderTest(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "putgetkey");

		console.info("Get record header: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);
		Record record = client.getHeader(params.policy, key);

		if (record == null) {
			throw new Exception(String.format(
					"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
		}

		// Generation should be greater than zero.  Make sure it's populated.
		if (record.generation == 0) {
			throw new Exception(String.format(
					"Invalid record header: generation=%d expiration=%d", record.generation, record.expiration));
		}
		console.info("Received: generation=%d expiration=%d", record.generation, record.expiration);
	}
}
