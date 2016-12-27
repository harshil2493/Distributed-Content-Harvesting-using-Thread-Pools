package cs455.harvester;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ThreadPoolManager implements Runnable
{
	String root;
	int poolSize;
	Crawler crawler;
	Queue<WorkerThread> workerQueue = new LinkedList<WorkerThread>();
	Queue<Task> taskList = new LinkedList<Task>();
	Queue<Task> handOffList = new LinkedList<Task>();
	TaskRegistry registry = new TaskRegistry();
	Map<String, Set<String>> outNodeVer = new HashMap<String, Set<String>>();
	Map<String, Set<String>> inNodeVer = new HashMap<String, Set<String>>();
	Set<String> brokenLinks = new HashSet<String>();
//	int counter;
	public ThreadPoolManager(Crawler crawler, String rootURL, int threadPoolSize) 
	{
		// TODO Auto-generated constructor stub
		this.crawler = crawler;
		this.root = rootURL;
		this.poolSize = threadPoolSize;
//		this.counter = threadPoolSize;
		for(int iteration = 0; iteration < threadPoolSize; iteration++)
		{
			WorkerThread threadObject = new WorkerThread(this);
			workerQueue.add(threadObject);
			
			Thread workingThread = new Thread(threadObject);
			workingThread.start();
			
		}
		synchronized (workerQueue) {
			workerQueue.notifyAll();
		}
		
		//startTasks();
	}

	public void startTasks()
	{
		// TODO Auto-generated method stub
		System.out.println("Task Assignment Has Been Started For Root : " + root + ". Threads Going To Be Used Are " + poolSize);
		System.out.println("Wait For All Threads To Complete Tasks .. You Will Be Notified When It Is Done");
		Task rootTask = new Task(root, 5, this, registry);
		synchronized (taskList) {
			taskList.add(rootTask);
			taskList.notifyAll();
			registry.futureTask.add(root);
		}
		
		Thread managerThread = new Thread(this);
		managerThread.start();		
	}

	@Override
	public void run() 
	{
		// TODO Auto-generated method stub
		
		while(true)
		{
			Task taskToPerform;
			synchronized (taskList)
			{
				if(taskList.isEmpty())
				{
					try {
						taskList.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				taskToPerform = taskList.poll();
				
				
			}
			
			WorkerThread worker;
			synchronized (workerQueue) 
			{
				if(workerQueue.isEmpty())
				{
					try {
						workerQueue.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				worker = workerQueue.poll();
			}
			
							worker.notifyMe(taskToPerform);
						
					
		}
		
	}

	public void storeStructure() throws IOException 
	{
		// TODO Auto-generated method stub
		System.out.println("Task Done : " + registry.futureTask.size());
		System.out.println("Data Structure Creation Initiated");
		
//		deleteDirectory("");
		root = root.split(".edu")[0] + ".edu";
//		System.out.println(root);
//		if(root.endsWith("/")) root = root.substring(0, root.length() - 1);
		String remove = root;
		root = root.replaceFirst("http://", "");
		
//		if(root.endsWith("/")) root = root.substring(0, root.length() - 1);		
//		File fileToDelete = new File("");
		
		String prePath = "/tmp/cs455-hkshah/" + root + "/";
		
//		System.out.println(deleteDirectory(new File(prePath)));
		if(true)
		{
		String nodes = "nodes/";
//		System.out.println(remove);
//		System.out.println(outNodeVer.size());
		for(String keys : outNodeVer.keySet())
		{
//			System.out.println("Original " + keys);
			if(!brokenLinks.contains(keys))
			{
			String originalKey = keys;
//			if(!keys.endsWith("/")) keys = keys + "/";
			if(keys.contains("https")) keys = keys.replace("https", "http");
			keys = keys.replace(remove, "");
			
//			if(keys.endsWith("/")) keys = keys.substring(0, keys.length() - 1);
			if(keys.startsWith("/")) keys = keys.substring(1, keys.length());
			keys = keys.replace("/", "-");
			keys = keys.replace("?", "-");
			if(keys.equals("") || keys.equals("-") || keys.equals("#")) keys = root + "/";
			if(keys.contains(":")) keys = keys.replace(":", "-");
			String keyPath = prePath + nodes + keys;
//			System.out.println(keyPath);
			if(!keyPath.endsWith("/")) keyPath = keyPath + "/"; 
//			System.out.println(keyPath);
//			File fileIn = new File(keyPath + "in.txt");
//			System.out.println("For " + keyPath);
			boolean createdDir = new File(keyPath).mkdirs();
//			System.out.println(originalKey);
//			System.out.println(keyPath + "out.txt");
			File fileOut = new File(keyPath + "out");
			
			if(!fileOut.exists())
			{
				fileOut.createNewFile();
				PrintWriter printWriter = new PrintWriter(keyPath + "out");
				for(String writeInFile : outNodeVer.get(originalKey))
				{
					if(!brokenLinks.contains(writeInFile))
					{
						printWriter.println(writeInFile);
					
					}
//					printWriter.println(writeInFile);
				}
				printWriter.close();
			}
//			PrintWriter printWriter = new PrintWriter(keyPath + "out.txt");
//			for(String writeInFile : outNodeVer.get(originalKey))
//			{
//				printWriter.println(writeInFile);
//			}
//			printWriter.close();
			
//			System.out.println("Created");
//			PrintWriter printWriter = new PrintWriter(keyPath);
//			printWriter.write("Harshil");
//			System.out.println("Key: " + keys + " ListSize: " + outNodeVer.get(keys).size());
			}
		}
		System.out.println("Data Structure Created For OutVerTex Created");
		System.out.println("Please Wait For Storing InVerTex Data");
		for(String keys : inNodeVer.keySet())
		{
			if(!brokenLinks.contains(keys))
			{
//			System.out.println("Original " + keys);
			String originalKey = keys;
			
//			System.out.println(originalKey);
			if(keys.contains("https")) keys = keys.replace("https", "http");
			keys = keys.replace(remove, "");
			
//			if(keys.endsWith("/")) keys = keys.substring(0, keys.length() - 1);
			if(keys.startsWith("/")) keys = keys.substring(1, keys.length());
			keys = keys.replace("/", "-");
			keys = keys.replace("?", "-");
			if(keys.equals("") || keys.equals("-") || keys.equals("#")) keys = root + "/";
			if(keys.contains(":")) keys = keys.replace(":", "-");
			
			String keyPath = prePath + nodes + keys;
//			System.out.println(keyPath);
			if(!keyPath.endsWith("/")) keyPath = keyPath + "/"; 
//			System.out.println(keyPath);
//			File fileIn = new File(keyPath + "in.txt");
//			System.out.println("For " + keyPath);
			boolean createdDir = new File(keyPath).mkdirs();
			File fileIn = new File(keyPath + "in");
//			System.out.println(keyPath + "in.txt");
			if(!fileIn.exists())
			{
				fileIn.createNewFile();
				PrintWriter printWriter = new PrintWriter(keyPath + "in");
				for(String writeInFile : inNodeVer.get(originalKey))
				{
					
						printWriter.println(writeInFile);
					
					
				}
				printWriter.close();
			}
			
//			PrintWriter printWriter = new PrintWriter(keyPath + "out.txt");
//			for(String writeInFile : outNodeVer.get(originalKey))
//			{
//				printWriter.println(writeInFile);
//			}
//			printWriter.close();
			
//			System.out.println("Created");
//			PrintWriter printWriter = new PrintWriter(keyPath);
//			printWriter.write("Harshil");
//			System.out.println("Key: " + keys + " ListSize: " + outNodeVer.get(keys).size());
			}
		}
		System.out.println("Data Structure Created For InVertex Created");
		System.out.println("Please Wait For Storing OutVerText Data");
		File fileBroken = new File(prePath + "broken-links");
		if(!fileBroken.exists())
		{
			fileBroken.createNewFile();
			PrintWriter printWriter = new PrintWriter(prePath + "broken-links");
//			System.out.println(brokenLinks.size());
			for(String writeInFileBroken : brokenLinks)
			{
				printWriter.println(writeInFileBroken);
			}
			printWriter.close();
		}
		System.out.println("Data Structure Created For BrokenLinks Created");
		System.out.println("Please Wait For Generating DisJoint Graph");
		File fileDis = new File(prePath + "disjoint-graphs/");
		if(!fileDis.exists())
		{
			fileBroken.createNewFile();
			//createGraph();
		}
		System.out.println("Data Structure Created For Graph Created");
		System.out.println("Data Structure Creation Successful");
		crawler.endingTask = System.nanoTime();
		long executionTime = crawler.endingTask - crawler.startingTask;
		
		System.out.println("Crawler Has Taken (seconds):" + (double) (executionTime / 1000000000.0));
		System.exit(0);
//		System.out.println("In");
//		for(String inKeys : inNodeVer.keySet())
//		{
//			System.out.println("Key: " + inKeys + " ListSize: " + inNodeVer.get(inKeys).size());
//		}
		}
	}

	private void deleteDirectory(File file) {
		// TODO Auto-generated method stub

    	if(file.isDirectory()){
 
    		//directory is empty, then delete it
    		if(file.list().length==0){
 
    		   file.delete();
    		   System.out.println("Directory is deleted : " 
                                                 + file.getAbsolutePath());
 
    		}else{
 
    		   //list all the directory contents
        	   String files[] = file.list();
 
        	   for (String temp : files) {
        	      //construct the file structure
        	      File fileDelete = new File(file, temp);
 
        	      //recursive delete
        	      deleteDirectory(fileDelete);
        	   }
 
        	   //check the directory again, if empty then delete it
        	   if(file.list().length==0){
           	     file.delete();
        	     System.out.println("Directory is deleted : " 
                                                  + file.getAbsolutePath());
        	   }
    		}
 
    	}else{
    		//if file, then delete it
    		file.delete();
    		System.out.println("File is deleted : " + file.getAbsolutePath());
    	}
	}

	public static ThreadPoolManager getInstance() {
		// TODO Auto-generated method stub
		return null;
	}

	public void addTask(TCPConnection c, String task) throws IOException 
	{
		// TODO Auto-generated method stub
		
		
		Task childTaskH = new Task(task, 1, this, registry);
		
		synchronized (handOffList) {
			
				handOffList.add(childTaskH);
				System.out.println("HandOff Added | Crawler Status: " + handOffList.size());	
				
		}
		
//		if(senderH.endsWith("/")) senderH = senderH.substring(0, senderH.length() - 1);
		
		
		CrawlerGetACK ack = new CrawlerGetACK(root);
		c.sender.sendData(ack.getByte());
		if(crawler.statusComplete == true)
		{
			crawler.statusComplete = false;
			synchronized (crawler.rootToConnection) {
				for(String k : crawler.rootToConnection.keySet())
				{
					CrawlerSendNegativeStatus negative = new CrawlerSendNegativeStatus(crawler.rootURLTemp);
					crawler.rootToConnection.get(k).sender.sendData(negative.getByte());
				}
			}
			
			addAllHandOff();
			
		}
		
		
//		System.out.println("From: " + senderH + " ToDo: " + childTaskH.taskToDo);
	}

	public void completeCrawling() throws IOException 
	{
		// TODO Auto-generated method stub
		addAllHandOff();
		
//		sendOtherCrawlersStatus();
	}

	private synchronized void addAllHandOff() throws IOException 
	{
		
		
//		System.out.println("Remaining Hand Off Task Addded");
		int counterToDecide = 0;
		// TODO Auto-generated method stub
		synchronized (handOffList) 
		{
//			System.out.println("HandOff Tasks: " + handOffList.size());
			for(Task t : handOffList)
			{
				if(!registry.futureTask.contains(t.taskToDo))
				{
					counterToDecide++;
					synchronized (taskList) {
						taskList.add(t);
						taskList.notifyAll();
					}
				}
			}
			handOffList.clear();
//			System.out.println("After Hand Off Size: " + handOffList.size());
//			System.out.println("Counter After HandOff Addition " + counterToDecide);
			if(counterToDecide == 0)
			{
				System.out.println("Crawler Is Done With All The Task..");
				sendOtherCrawlersStatusTrue();
				synchronized (crawler) {
					crawler.statusComplete = true;	
				}
				synchronized (crawler) {
					if(crawler.totalCrawlersCompleted == crawler.totalCrawlers)
					{
						System.out.println("Storing Data Structure Now..");
						storeStructure();
					}
				}
				
			}
		}
//		sendOtherCrawlersStatusTrue();
	}

	private void sendOtherCrawlersStatusTrue() throws IOException 
	{
		// TODO Auto-generated method stub
		System.out.println("Notifying Other Crawlers");

		synchronized (crawler.ackCount) {
		
			
			if(crawler.ackCount == 0)
			{
				System.out.println("Acknowledgement Receive For All HandOffs");
			}
			else
			{
				System.out.println("Some Acknowledgement Are Yet To Receive For HandOffs");
			}
			
		}
		
		System.err.println("This Crawler Has Finished Crawling .. ");
//		System.err.println("It Will Wait For Other Crawlers To Get Done");
//		sendOtherCrawlersStatusTrue();
		
		for(String k : crawler.rootToConnection.keySet())
		{
			CrawlerSendPositiveStatus crawlerStatus = new CrawlerSendPositiveStatus(crawler.rootURLTemp);
			crawler.rootToConnection.get(k).sender.sendData(crawlerStatus.getByte());
		}
//		System.out.println(crawler.statusComplete);
	}

}
