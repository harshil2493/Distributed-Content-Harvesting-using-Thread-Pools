package cs455.harvester;

import java.net.Socket;


public class TCPConnection {
	public TCPReceiverThread receiver;
	public TCPSender sender;
	public Socket s;

	public TCPConnection(Crawler node, Socket socket) throws Exception {
		// TODO Auto-generated constructor stub
		receiver = new TCPReceiverThread(node, socket);
		sender = new TCPSender(socket);
		s = socket;
		Thread threadReceiver = new Thread(receiver);

		threadReceiver.start();

	}

}
