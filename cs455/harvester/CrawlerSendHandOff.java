package cs455.harvester;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CrawlerSendHandOff {
	Crawler c;
	String crawlingString;
	
	public CrawlerSendHandOff(Crawler crawler, String crawled) 
	{
		// TODO Auto-generated constructor stub
		c = crawler;
		crawlingString = crawled;
		
	}
	public byte[] getByte() throws IOException 
	{
		// TODO Auto-generated method stub
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(
				baOutputStream));
		dout.writeInt(Protocols.Crawler_Send_Hand_Off);

//		dout.writeInt(node.nodeID);
		
		byte[] rootByte = c.rootURLTemp.getBytes();
		byte[] crawlingByte = crawlingString.getBytes();
		
		dout.writeInt(rootByte.length);
		dout.write(rootByte);
		dout.writeInt(crawlingByte.length);
		dout.write(crawlingByte);
		
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
