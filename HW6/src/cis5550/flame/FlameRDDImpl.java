package cis5550.flame;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD{
	public String tableName = null;

	public FlameRDDImpl(String tableName) {
		// TODO Auto-generated constructor stub
		this.tableName = tableName;
	}

	@Override
	public List<String> collect() throws Exception {
		List<String> elements = new ArrayList<String>();
		Iterator<Row> tableScan = Coordinator.kvs.scan(tableName);
		while (tableScan.hasNext()) {
			Row next = tableScan.next();
			elements.add(next.get("value"));
		}
		return elements;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/flatMap", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1);
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/mapToPair", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1);
		FlamePairRDDImpl rddPairImpl = new FlamePairRDDImpl(newTableName);
		return rddPairImpl;
	}

	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		List<String> tableOne = new ArrayList<>(new HashSet<>(collect()));
		List<String> tableTwo = new ArrayList<>(new HashSet<>(r.collect()));
		List<String> common = new ArrayList<>(tableOne);
		common.retainAll(tableTwo);
		KVSClient kvs = Coordinator.kvs;
		String newTableName = UUID.randomUUID().toString();
		int i = 0;
		for (String s : common) {
			kvs.put(newTableName, Hasher.hash(Integer.toString(i)), "value", s);
			i++;
		}
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/sampling", "POST", tableName, null, null, f);
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
//		String newTableName = FlameContextImpl.invokeOperation("/rdd/groupBy", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1);
//		FlamePairRDDImpl rddPairImpl = new FlamePairRDDImpl(newTableName);
//		return rddPairImpl;
		return null;
	}

}
