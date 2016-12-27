package cs455.harvester;

import java.io.DataInputStream;
import java.net.Socket;


public class TCPReceiverThread implements Runnable {

	private Socket socket;
	private DataInputStream dataInputStream;
	private Crawler node;

	TCPReceiverThread(Crawler node, Socket socket) throws Exception {
		this.socket = socket;
		this.dataInputStream = new DataInputStream(socket.getInputStream());
		this.node = node;
	}

	public void run() {
		try {
			while (true) {
				byte[] rawBytes = this.receive();

			}
		} catch (Exception e) {
		}
	}

	byte[] receive() throws Exception {
		int dataLength;
		dataLength = dataInputStream.readInt();
		byte[] data = new byte[dataLength];
		dataInputStream.readFully(data, 0, dataLength);
		node.onEvent(data);
		return data;
	}

}
