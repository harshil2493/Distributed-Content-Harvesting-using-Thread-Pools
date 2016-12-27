package cs455.harvester;

import java.net.ServerSocket;
import java.net.Socket;


public class TCPServerThread implements Runnable {
	int serverPortNumber;
	Crawler server;
	String hostOfCrawler;
	public TCPServerThread(String host, String portNumber, Crawler requestingServer) {
		// TODO Auto-generated constructor stub
		serverPortNumber = Integer.parseInt(portNumber);
		server = requestingServer;
		hostOfCrawler = host;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			
			ServerSocket serverSocket = new ServerSocket(serverPortNumber);
			System.out
					.println("Crawler Is Online Now On PORT: "+ serverSocket.getLocalPort() + " HOST: "+ serverSocket.getInetAddress().getLocalHost().getHostName() +"..! Other Crawlers Can Send Requests!");
			while (true) {
				Socket socket = serverSocket.accept();

				TCPConnection tcpConnection = new TCPConnection(server, socket);

				//Store Connection In DataBase
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Registry Might Be Already Live On Same Port");

		}
	}

}
