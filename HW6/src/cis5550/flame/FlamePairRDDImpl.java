package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD{
	public String tableName = null;

	public FlamePairRDDImpl(String tableName) {
		// TODO Auto-generated constructor stub
		this.tableName = tableName;
	}

	@Override
	public List<FlamePair> collect() throws Exception {
		List<FlamePair> elements = new ArrayList<FlamePair>();
		Iterator<Row> tableScan = Coordinator.kvs.scan(tableName);
		while (tableScan.hasNext()) {
			Row next = tableScan.next();
			String rowKey = next.key();
			Set<String> columns = next.columns();
			for (String c : columns) {
				String value = next.get(c);
				elements.add(new FlamePair(rowKey, value));
			}
		}
		return elements;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		// TODO Auto-generated method stub
		String newTableName = FlameContextImpl.invokeOperation("/rdd/foldByKey", "POST", tableName, zeroElement, Serializer.objectToByteArray(lambda), -1);
		FlamePairRDDImpl rddPairImpl = new FlamePairRDDImpl(newTableName);
		return rddPairImpl;
	}
}
