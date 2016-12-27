package cs455.harvester;

import java.io.DataOutputStream;
import java.net.Socket;

public class TCPSender {

	private Socket socket;
	private DataOutputStream dataOutputStream;

	TCPSender(Socket socket) throws Exception {
		this.socket = socket;
		this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
	}

	public void sendData(byte[] data) {
		int dataLength = data.length;
		try {
			dataOutputStream.writeInt(dataLength);
			dataOutputStream.write(data, 0, dataLength);
			dataOutputStream.flush();

		} catch (Exception e) {
		}
	}
}
