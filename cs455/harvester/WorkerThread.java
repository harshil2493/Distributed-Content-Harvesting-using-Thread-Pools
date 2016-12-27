package cs455.harvester;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

public class WorkerThread implements Runnable {
	Object lockingObject = new Object();
	Task toDoTask;
	ThreadPoolManager poolManager;

	public WorkerThread(ThreadPoolManager threadPoolManager) 
	{
		// TODO Auto-generated constructor stub
		poolManager = threadPoolManager;
	}

	@Override
	public void run() 
	{
		// TODO Auto-generated method stub
		synchronized (lockingObject) 
		{
		while (true) 
		{
			
			try 
			{
				
//					System.out.println("Waiting " + this);
					
					
					
//				System.out.println("Before Wait: " + poolManager.workerQueue.size());
//				System.out.println("Remaining " + poolManager.taskList.size() + " Workers : " + poolManager.workerQueue.size());
//				if(poolManager.taskList.size() == 0)
//				{
//						System.out.println("Done" + poolManager.workerQueue.size());
//				}
				lockingObject.wait();
				
//				System.out.println("Thread: " + this + " Executing " + toDoTask.taskToDo);
				try 
				{

//					System.out.println(poolManager.registry.futureTask.size());
//							
//					System.out.println("Thread: " + this + " Executing " + toDoTask.taskToDo);
					toDoTask.executeTask();
				} 
				catch (IOException ioe) {
					// TODO Auto-generated catch block
					if(ioe instanceof ConnectException)
					{
						System.err.println("Broken Link Due To ConnectionTimeOut.. That Is Why Adding It To Broken Link: " + toDoTask.taskToDo);

					}
					else
					{
						System.err.println("Broken Link Due To IO: " + toDoTask.taskToDo);
					}
//					System.out.println("Broken Link Due To IO: " + toDoTask.taskToDo);
					synchronized (poolManager.brokenLinks)
					{
						poolManager.brokenLinks.add(toDoTask.taskToDo);
					}
//					ioe.printStackTrace();
				} 
				catch (URISyntaxException e) {
					// TODO: handle exception
					System.err.println("Broken Link Due To URISyntax:  " + toDoTask.taskToDo);
					synchronized (poolManager.brokenLinks)
					{
						poolManager.brokenLinks.add(toDoTask.taskToDo);
					}
//					e.printStackTrace();
				} 
				catch (OutOfMemoryError e) {
					// TODO: handle exception
//					System.err.println("OutOfMemoryError Occured During Executing "+toDoTask.taskToDo);
				}
				finally 
				{
					

					Thread.sleep(1000);
					
					synchronized (poolManager.workerQueue) 
					{
						poolManager.workerQueue.add(this);
						
						poolManager.workerQueue.notifyAll();
						synchronized (poolManager.taskList) 
						{
//							System.out.println("Active Threads : " + poolManager.workerQueue.size() + " Task Remaining" + poolManager.taskList.size());
							if(poolManager.taskList.size() == 0 && poolManager.workerQueue.size() == poolManager.poolSize)
							{
//								System.out.println("Crawling For "+ poolManager.root +" Is Successful .. With The Use Of "  + poolManager.workerQueue.size() + " Threads");
								//									poolManager.storeStructure();
								try 
								{
									poolManager.completeCrawling();
								} 
								catch (IOException e)
								{
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					}
					
				}

			} 
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				
//				synchronized (poolManager.workerQueue) 
//				{
//					poolManager.workerQueue.add(this);
//				}
				
			}

		}
		}
	}

	public void notifyMe(Task taskToPerform) {
		// TODO Auto-generated method stub
		toDoTask = taskToPerform;
		synchronized (lockingObject) 
		{
//			System.out.println("Notifying " + this);
			lockingObject.notify();
		}

	}

}
