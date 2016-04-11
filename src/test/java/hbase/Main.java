package hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Main {
	private static Connection conn = null;
	private static HTable table = null;

	@Before
	public void init() throws IOException {
		System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.150.27.219:2181,10.150.27.223:2181");
		conn = ConnectionFactory.createConnection(conf);
		table = (HTable) conn.getTable(TableName.valueOf("test"));
	}

	@Test
	public void get() throws IOException {
		Get get = new Get(Bytes.toBytes("row1"));
		Result result = table.get(get);
		byte[] bs = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("cf1"));
		System.out.println(Bytes.toString(bs));
	}

	@Test
	public void scan() throws IOException {
		Scan scan = new Scan(Bytes.toBytes("row1"), Bytes.toBytes("row2"));
		ResultScanner results = table.getScanner(scan);
		Iterator<Result> rs = results.iterator();
		while (rs.hasNext()) {
			byte[] bs = rs.next().getValue(Bytes.toBytes("cf"), Bytes.toBytes("cf1"));
			System.out.println(Bytes.toString(bs));
		}

	}

	@After
	public void clear() throws IOException {
		conn.close();
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		String path = "http://c.hiphotos.baidu.com/image/pic/item/f3d3572c11dfa9ec78e256df60d0f703908fc12e.jpg";
		URL u = new URL(path);
		InputStream input =u.openStream();
		ByteArrayOutputStream out =  new ByteArrayOutputStream();
		IOUtils.copy(input, out);
		input.close();
		String s = Bytes.toStringBinary(out.toByteArray());
		out.close();
		//s=Bytes.toString(Bytes.toBytesBinary(s)); 
		System.out.println(s);
	}
}
