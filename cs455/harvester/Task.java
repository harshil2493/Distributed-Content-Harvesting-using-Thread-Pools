package cs455.harvester;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import net.htmlparser.jericho.Config;
import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.LoggerProvider;
import net.htmlparser.jericho.Source;


public class Task {
	String taskToDo;
	int depth;
	ThreadPoolManager manager;
	String mainRoot;
	TaskRegistry taskRegistry;
	Set<String> outNode = new HashSet<String>();

	public Task(String toDO, int deep, ThreadPoolManager threadPoolManager,
			TaskRegistry registry) {
		// TODO Auto-generated constructor stub
		taskToDo = toDO;
		depth = deep;
		manager = threadPoolManager;
		mainRoot = manager.root;
		taskRegistry = registry;

	}

	public static boolean checkDomain(String pageUrl, String rootUrl)
			throws MalformedURLException {
		
		return new URL(pageUrl).getHost().equals(new URL(rootUrl).getHost());
	}

	public void executeTask() throws MalformedURLException, IOException,
			URISyntaxException {

		// TODO Auto-generated method stub
		Config.LoggerProvider = LoggerProvider.DISABLED;
		// taskToDo = resolveURL(taskToDo);
		// System.out.println(taskToDo);
		// System.out.println("Crawling Started For " + taskToDo);
		
			HttpURLConnection con = (HttpURLConnection) (new URL(taskToDo)
					.openConnection());
			con.connect();
			InputStream is = con.getInputStream();
			String temp = con.getURL().toString();
			Source source = new Source(is);
			if (!temp.equals(taskToDo)) {
				synchronized (taskRegistry.futureTask) {
					taskRegistry.futureTask.add(temp);
				}
			}
			taskToDo = temp;
			
			List<Element> aTags = source.getAllElements(HTMLElementName.A);
			try
			{
			if (depth > 0) {
			for (Element aTag : aTags) {

				String crawled = aTag.getAttributeValue("href");
				crawled = normalized(crawled);

				// crawled = crawled.replaceAll("[", "");

				if (crawled != null) {
					// System.out.println(crawled);
					if (new URI(crawled).isAbsolute()) {
					} else {

						// System.out.println("Before : " + crawled);
						// System.out.println(" Before " + crawled + ":" +
						// Thread.currentThread().getId());
						// URI resolvedC = new URI(taskToDo).resolve(crawled);
						URL resolvedC = new URL(new URL(taskToDo), crawled);
						crawled = resolvedC.toString();
					}
					crawled = normalFormed(crawled);
					// checkDomain(crawled, mainRoot);
					// System.out.println(mainRoot);
					// if(crawled.endsWith("/")) crawled = crawled.substring(0,
					// crawled.length() - 1);
					// if(crawled.endsWith("/") || crawled.endsWith("#"))
					// crawled = crawled.substring(0, crawled.length() - 1);
					if (checkDomain(crawled, mainRoot)) {

						// crawled = resolveURL(crawled);
						synchronized (outNode) {
							outNode.add(crawled);
						}
						if (endOfLink(crawled)) {
							synchronized (manager.inNodeVer) {
								Set<String> finalToAddToCrawled = manager.inNodeVer
										.get(crawled);
								if (finalToAddToCrawled == null)
									finalToAddToCrawled = new HashSet<String>();
								finalToAddToCrawled.add(taskToDo);
								manager.inNodeVer.put(crawled,
										finalToAddToCrawled);
							}

							// if(crawled.contains("#")) crawled =
							// crawled.substring(0, crawled.lastIndexOf("#"));
							// if(crawled.endsWith("/")) crawled =
							// crawled.substring(0, crawled.length() - 1);
							// if(crawled.contains("?")) crawled =
							// crawled.substring(0, crawled.lastIndexOf("?"));
							if (!taskRegistry.futureTask.contains(crawled)) {
								// System.out.println(crawled);
								synchronized (taskRegistry.futureTask) {
									taskRegistry.futureTask.add(crawled);
								}

								Task childTask = new Task(crawled, depth - 1,
										manager, taskRegistry);
								synchronized (manager.taskList) {
									manager.taskList.add(childTask);
									manager.taskList.notifyAll();
								}

							}
						}
					} else {
						
							
							String handOffDomain = getDomainForHand(crawled);

							if (handOffDomain != null) {
								// System.out.println("HandOff Of " + crawled +
								// " Given To " + handOffDomain);
								// System.out.println("Called From Here For " +
								// crawled);
								synchronized (outNode) {
									outNode.add(crawled);
								}
								
								
								if (!taskRegistry.handOffTask.contains(crawled)) {
									
									synchronized (taskRegistry.handOffTask) {
										taskRegistry.handOffTask.add(crawled);
									}
								CrawlerSendHandOff handOff = new CrawlerSendHandOff(
										manager.crawler, crawled);
								// byte[] data = ;
								synchronized (manager.crawler.ackCount) {
									manager.crawler.ackCount++;
								}
								synchronized (manager.crawler.rootToConnection) {
									manager.crawler.rootToConnection
											.get(handOffDomain).sender
											.sendData(handOff.getByte());
								}

							}
						}

					}
				}
			}
			
			synchronized (manager.outNodeVer) {
				
				manager.outNodeVer.put(taskToDo,
						outNode);
			}
		}
		else if (depth == 0) {
			for (Element aTag : aTags) {

				String crawled = aTag.getAttributeValue("href");
				crawled = normalized(crawled);

				// crawled = crawled.replaceAll("[", "");

				if (crawled != null) {
					// System.out.println(crawled);
					if (new URI(crawled).isAbsolute()) {
					} else {

						// System.out.println("Before : " + crawled);
						// System.out.println(" Before " + crawled + ":" +
						// Thread.currentThread().getId());
						// URI resolvedC = new URI(taskToDo).resolve(crawled);
						URL resolvedC = new URL(new URL(taskToDo), crawled);
						crawled = resolvedC.toString();
					}
					crawled = normalFormed(crawled);
					// checkDomain(crawled, mainRoot);
					// System.out.println(mainRoot);
					// if(crawled.endsWith("/")) crawled = crawled.substring(0,
					// crawled.length() - 1);
					// if(crawled.endsWith("/") || crawled.endsWith("#"))
					// crawled = crawled.substring(0, crawled.length() - 1);
					if (checkDomain(crawled, mainRoot)) {

						// crawled = resolveURL(crawled);
						synchronized (outNode) {
							outNode.add(crawled);
						}
						if (endOfLink(crawled)) {
							synchronized (manager.inNodeVer) {
								Set<String> finalToAddToCrawled = manager.inNodeVer
										.get(crawled);
								if (finalToAddToCrawled == null)
									finalToAddToCrawled = new HashSet<String>();
								finalToAddToCrawled.add(taskToDo);
								manager.inNodeVer.put(crawled,
										finalToAddToCrawled);
							}

							// if(crawled.contains("#")) crawled =
							// crawled.substring(0, crawled.lastIndexOf("#"));
							// if(crawled.endsWith("/")) crawled =
							// crawled.substring(0, crawled.length() - 1);
							// if(crawled.contains("?")) crawled =
							// crawled.substring(0, crawled.lastIndexOf("?"));
//							if (!taskRegistry.futureTask.contains(crawled)) {
//								// System.out.println(crawled);
//								synchronized (taskRegistry.futureTask) {
//									taskRegistry.futureTask.add(crawled);
//								}
//
//								Task childTask = new Task(crawled, depth - 1,
//										manager, taskRegistry);
//								synchronized (manager.taskList) {
//									manager.taskList.add(childTask);
//									manager.taskList.notifyAll();
//								}
//
//							}
						}
					} else {
						

					}
				}
			}
synchronized (manager.outNodeVer) {
				
				manager.outNodeVer.put(taskToDo,
						outNode);
			}
		}
			}
			catch(Exception e)
			{
				
			}
			is.close();

	}

	private String normalFormed(String normalized) {
		// TODO Auto-generated method stub
		
			 
	        if (normalized == null) {
	            return null;
	        }
	 
	        // If the buffer begins with "./" or "../", the "." or ".." is removed.
	        if (normalized.startsWith("./")) {
	            normalized = normalized.substring(1);
	        } else if (normalized.startsWith("../")) {
	            normalized = normalized.substring(2);
	        } else if (normalized.startsWith("..")) {
	            normalized = normalized.substring(2);
	        }
	 
	        // All occurrences of "/./" in the buffer are replaced with "/"
	        int index = -1;
	        while ((index = normalized.indexOf("/./")) != -1) {
	            normalized = normalized.substring(0, index) + normalized.substring(index + 2);
	        }
	 
	        // If the buffer ends with "/.", the "." is removed.
	        if (normalized.endsWith("/.")) {
	            normalized = normalized.substring(0, normalized.length() - 1);
	        }
	 
	        int startIndex = 0;
	 
	        // All occurrences of "/<segment>/../" in the buffer, where ".."
	        // and <segment> are complete path segments, are iteratively replaced
	        // with "/" in order from left to right until no matching pattern remains.
	        // If the buffer ends with "/<segment>/..", that is also replaced
	        // with "/".  Note that <segment> may be empty.
	        while ((index = normalized.indexOf("/../", startIndex)) != -1) {
	            int slashIndex = normalized.lastIndexOf('/', index - 1);
	            if (slashIndex >= 0) {
	                normalized = normalized.substring(0, slashIndex) + normalized.substring(index + 3);
	            } else {
	                startIndex = index + 3;
	            }
	        }
	        if (normalized.endsWith("/..")) {
	            int slashIndex = normalized.lastIndexOf('/', normalized.length() - 4);
	            if (slashIndex >= 0) {
	                normalized = normalized.substring(0, slashIndex + 1);
	            }
	        }
	 
	        // All prefixes of "<segment>/../" in the buffer, where ".."
	        // and <segment> are complete path segments, are iteratively replaced
	        // with "/" in order from left to right until no matching pattern remains.
	        // If the buffer ends with "<segment>/..", that is also replaced
	        // with "/".  Note that <segment> may be empty.
	        while ((index = normalized.indexOf("/../")) != -1) {
	            int slashIndex = normalized.lastIndexOf('/', index - 1);
	            if (slashIndex >= 0) {
	                break;
	            } else {
	                normalized = normalized.substring(index + 3);
	            }
	        }
	        if (normalized.endsWith("/..")) {
	            int slashIndex = normalized.lastIndexOf('/', normalized.length() - 4);
	            if (slashIndex < 0) {
	                normalized = "/";
	            }
	        }
	 
	        return normalized;
	    
	}

	private String getDomainForHand(String crawled)
			throws MalformedURLException {
		// TODO Auto-generated method stub
		for (String root : manager.crawler.rootToConnection.keySet()) {
			if (new URL(root).getHost().equals(new URL(crawled).getHost()))
				return root;
		}

		return null;
	}

	private String normalized(String crawled) {
		// TODO Auto-generated method stub
		if (crawled != null) {
			crawled = crawled.trim();

			if (crawled.contains("]"))
				crawled = crawled.replaceAll("]", "");

			if (crawled.contains(" "))
				crawled = crawled.replaceAll(" ", "%20");
			if (crawled.contains("\n"))
				crawled = crawled.replaceAll("\n", "");
			if (crawled.contains("\r"))
				crawled = crawled.replaceAll("\r", "");
			if (crawled.contains("#"))
				crawled = crawled.substring(0, crawled.indexOf("#"));
			if (crawled.contains("?"))
				crawled = crawled.substring(0, crawled.indexOf("?"));

			// if (crawled.contains("[\]"))
			// crawled = crawled.replace("[\\]", "");
			if (crawled.contains("\\"))
				crawled = crawled.substring(0, crawled.indexOf("\\"))
						+ crawled.substring(crawled.indexOf("\\") + 1,
								crawled.length());
			if (crawled.contains("\""))
				crawled = crawled.replaceAll("\"", "");
			if (crawled.contains("\t"))
				crawled = crawled.replaceAll("\t", "");
			if (crawled.contains("["))
				crawled = crawled.replace("[", "");
			if (crawled.length() > 4 && crawled.charAt(4) == '|')
				crawled = crawled.replace("|", ":");

			if ((crawled.length() == 7 && crawled.contains("http://"))
					
					|| crawled.startsWith("mail")
					|| crawled.contains("file://")
					|| (crawled.startsWith("{{") && crawled.endsWith("}}")))
				crawled = null;
			// System.out.println(crawled);

		}
		return crawled;
	}

	private boolean endOfLink(String crawled) {
		// TODO Auto-generated method stub
		int lastIndexOf = crawled.lastIndexOf("/");
		String tmp = crawled.substring(lastIndexOf, crawled.length());
		// System.out.println("-tmp-" + tmp);
		if (tmp.contains(".")) {
			if ((tmp.endsWith("html") || tmp.endsWith("htm")
					|| tmp.endsWith("cfm") || tmp.endsWith("aspx")
					|| tmp.endsWith("php") || tmp.endsWith("asp") || tmp
						.endsWith("jsp"))
					&& !(crawled.contains("https://")
							|| crawled.contains("file://") || crawled
								.contains("mailto"))) {
				return true;
			}
		} else {
			return true;
		}
		return false;

	}

	private String resolveURL(String url) throws MalformedURLException,
			IOException {
		// TODO Auto-generated method stub

		HttpURLConnection con = (HttpURLConnection) (new URL(url)
				.openConnection());
		con.setInstanceFollowRedirects(false);
		con.connect();
		int responseCode = con.getResponseCode();
		if (responseCode == 301) {
			return con.getHeaderField("Location");
		} else {
			return url;
		}
	}

}
