package cs455.harvester;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CrawlerSendPositiveStatus {
	String r;
	public CrawlerSendPositiveStatus(String rootURLTemp) {
		// TODO Auto-generated constructor stub
		r = rootURLTemp;
	}

	public byte[] getByte() throws IOException {
		// TODO Auto-generated method stub
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(
				baOutputStream));
		dout.writeInt(Protocols.Crawler_Complete_Status);

//		dout.writeInt(node.nodeID);
		
		byte[] rootByte = r.getBytes();
		
		dout.writeInt(rootByte.length);
		dout.write(rootByte);
		
//		System.out.println("Crawling String: " + crawlingString);
//		System.out.println("Crawling Bytes: " + crawlingByte);
//		System.out.println("Crawling StringToByte: " + new String(crawlingByte));
//		System.out.println("Crawling StringToByte Len: " + (crawlingByte).length);

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();

		return marshalledBytes;
	}

}
