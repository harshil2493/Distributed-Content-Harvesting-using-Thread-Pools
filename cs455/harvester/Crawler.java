package cs455.harvester;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


public class Crawler 
{
	Map<String, String> rootToIPPort = new HashMap<String, String>();
	Map<String, TCPConnection> rootToConnection = new HashMap<String, TCPConnection>();
//	static String rootURL;
	
	public Integer totalCrawlersCompleted = 0;
	public Integer totalCrawlers = 0;
	public static String rootURLTemp;
	ThreadPoolManager manager;
	public boolean statusComplete = false;
	public Integer ackCount = 0;
	long startingTask;
	long endingTask;
	public Object lockerForConnection = new Object(); 
	public Crawler(int portNumber, int threadPoolSize, String rootURL, String pathToConfigFile) throws Exception 
	{
		startingTask = System.nanoTime();
		// TODO Auto-generated constructor stub
		manager = new ThreadPoolManager(this, rootURL, threadPoolSize); 
//		manager.startTasks();
//		System.out.println(manager);
		BufferedReader br = null;
		 
		try 
		{
 
			String sCurrentLine;
 
			br = new BufferedReader(new FileReader(pathToConfigFile));
			
			while ((sCurrentLine = br.readLine()) != null) 
			{
				String temp[] = sCurrentLine.split(",");
				temp[1] = resolveIt(temp[1]);
				if(temp[1].endsWith("/")) temp[1] = temp[1].substring(0, temp[1].length() - 1);
				rootToIPPort.put(temp[1], temp[0]);
				totalCrawlers++;
			}
			totalCrawlers--;
 
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		finally
		{
			try 
			{
				if (br != null)br.close();
			} 
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
//		System.out.println(rootToIPPort);
//		runThreadForHandOff("Host", "0", rootToIPPort);
		for(String key : rootToIPPort.keySet())
		{
			if(rootURL.contains(key))
			{
				String IPPort[] = (rootToIPPort.get(key)).split(":");
				
				
				
				runThreadForHandOff(IPPort[0], IPPort[1], rootToIPPort, manager);
				break;
				
			}
		}
		
		
	}

	private String resolveIt(String rootURL) throws MalformedURLException, IOException {
		// TODO Auto-generated method stub
		 HttpURLConnection con = (HttpURLConnection)(new URL(rootURL).openConnection());
		  con.connect();
		  InputStream is = con.getInputStream();
		  // this is the actual url, the page is redirected to (if there is a redirect).
		  String redirectedUrl = con.getURL().toString();
		  is.close();
		  return redirectedUrl;
		  
	}

	private void runThreadForHandOff(String host, String portNumber, Map<String, String> rootToIPPort, ThreadPoolManager manager) throws Exception
	{
		
		// TODO Auto-generated method stub
		String toRemove = host+":"+portNumber;
		
		TCPServerThread serverThread = new TCPServerThread(host, portNumber, this);
		Thread threadServerRunning = new Thread(serverThread);
		threadServerRunning.start();
		
		try 
		{
			Thread.sleep(10000);
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("This Crawler Will Now Initiate Connection To Other Crawlers");
		connectToOtherCrawlers(rootToIPPort, manager);
	}

	private void connectToOtherCrawlers(Map<String, String> rootToIPPortN, ThreadPoolManager m) throws Exception 
	{
		// TODO Auto-generated method stub
//		System.out.println(rootURLTemp);
//		if(rootURLTemp.endsWith("/")) rootURLTemp = rootURLTemp.substring(0, rootURLTemp.length() - 1);
		synchronized (rootToIPPortN) {
			for(String keyToConnect : rootToIPPortN.keySet())
			{
//				System.out.println(keyToConnect + " " + rootToIPPortN.get(keyToConnect));
				if(!rootURLTemp.contains(keyToConnect))
				{

//					System.out.println(keyToConnect + " " + rootToIPPortN.get(keyToConnect));
					String tempPort[] = rootToIPPortN.get(keyToConnect).split(":");
					int port = Integer.parseInt(tempPort[1]);
					String host = tempPort[0];
					Socket s = new Socket(host, port);
					TCPConnection connection = new TCPConnection(this, s);
					rootToConnection.put(keyToConnect, connection);
					System.out.println("Connected To " + keyToConnect+" Crawler");
					
					
				}
			}
//			System.out.println(rootToConnection);
			synchronized (lockerForConnection) {
				lockerForConnection.notifyAll();	
			}
			
			//			System.out.println(rootToConnection);
			
			m.startTasks();
			
		}
		
	}

	public static void main(String argument[]) throws Exception 
	{
		int portNumber = Integer.parseInt(argument[0]);
		int threadPoolSize = Integer.parseInt(argument[1]);
//		int threadPoolSize = 16;
		String rootURL = argument[2];
//		String rootURL = "http://www.chm.colostate.edu";
//		String rootURL = "http://www.bmb.colostate.edu";
//		String rootURL = "http://www.biology.colostate.edu/";
//		String rootURL = "http://www.math.colostate.edu";
//		String rootURL = "http://www.stat.colostate.edu";
		 HttpURLConnection con = (HttpURLConnection)(new URL(rootURL).openConnection());
		  con.connect();
		  InputStream is = con.getInputStream();
		  // this is the actual url, the page is redirected to (if there is a redirect).
		  String redirectedUrl = con.getURL().toString();
		  is.close();
		  rootURL = redirectedUrl;
		  if(!rootURL.endsWith("/") && !rootURL.contains(".php")) rootURL = rootURL + "/";
		rootURLTemp = rootURL;
		
		String pathToConfigFile = argument[3];
		System.out.println("Crawler For: " + rootURLTemp);
		Crawler crawler = new Crawler(portNumber, threadPoolSize, rootURL, pathToConfigFile);
	}

	public void onEvent(byte[] data) throws IOException, InterruptedException 
	{
		// TODO Auto-generated method stub
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(
				data);
		DataInputStream dataInputStream = new DataInputStream(
				new BufferedInputStream(baInputStream));
		int type = dataInputStream.readInt();
		if(type == Protocols.Crawler_Send_Hand_Off)
		{
			int sizeOfSender = dataInputStream.readInt();
			
			byte[] senderC = new byte[sizeOfSender];
			dataInputStream.readFully(senderC);
			int taskToDo = dataInputStream.readInt();
			byte[] needToDo = new byte[taskToDo];
			
//			System.out.println("Rec Bytes: " + needToDo.length);
			dataInputStream.readFully(needToDo);
			
//			ThreadPoolManager poolManager = ThreadPoolManager.getInstance();
//			Task childTask = new Task(new String(needToDo), 1, manager, manager.registry);
			
//			System.out.println("From: " + new String(senderC) + " ToDo: " + new String(needToDo));
			String senderH = new String(senderC);
			if(senderH.endsWith("/")) senderH = senderH.substring(0, senderH.length() - 1);
			TCPConnection tcpConnection;
			synchronized (rootToConnection) {
//				System.out.println(senderH);
//				System.out.println(crawler.rootToConnection.get(senderH));
				if(rootToConnection.isEmpty()) 
				{
					synchronized (lockerForConnection) {
						System.out.println("Waiting For Proper SetUp Of Connection .. Even Though Hand Of Is Given To This Crawler");
						lockerForConnection.wait();
						System.out.println("Waiting Released");
					}
				}
				tcpConnection = rootToConnection.get(senderH);	
		}
//			System.out.println("Sending To: " + senderH + "Connection: " + tcpConnection);
			manager.addTask(tcpConnection, new String(needToDo));
			
		}
		else if(type == Protocols.Crawler_Get_Ack)
		{
//			System.out.println("ACK"+ackCount);
			synchronized (ackCount) {
				ackCount--;
				if(ackCount == 0)
				{
					System.out.println("All Recent's HandOff Ack Recieved ");
				}
			}
			
		}
		else if(type == Protocols.Crawler_Complete_Status)
		{
//			System.out.println("Completion Report Recieved");
			synchronized (totalCrawlersCompleted) {
				totalCrawlersCompleted++;
				int sizeOfCrawlerReported = dataInputStream.readInt();
				byte[] rootCraw = new byte[sizeOfCrawlerReported];
				dataInputStream.readFully(rootCraw);
				System.out.println("Completion Report: " + new String(rootCraw));
				System.out.println("Total Completed Crawlers : " + totalCrawlersCompleted);
				
			}
			synchronized (this) {
				if(this.statusComplete == true && this.totalCrawlersCompleted == this.totalCrawlers)
				{
					System.out.println("Storing Data Structure Now..");
					manager.storeStructure();
				}
			}
		}
		else if(type == Protocols.Crawler_Incomplete_Status)
		{
//			System.out.println("Called?");
			int sizeOfCrawlerReportedN = dataInputStream.readInt();
			byte[] rootCrawN = new byte[sizeOfCrawlerReportedN];
			dataInputStream.readFully(rootCrawN);
			System.out.println("InCompletion Report Of Crawler " + new String(rootCrawN));
			synchronized (totalCrawlersCompleted) {
				totalCrawlersCompleted--;
			}
		}
		dataInputStream.close();
		baInputStream.close();
	}
}
