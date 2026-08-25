package Components.Infra;

import java.io.IOException;

public class Slave {
    public Client connection;
    public java.util.List<String> capabilities;

    public Slave(Client client){
        this.connection = client;
        this.capabilities = new java.util.ArrayList<>();
    }

    public void send(byte[] bytes) throws IOException {
        if(bytes!=null){
            this.connection.outputStream.write(bytes);
        }
    }
}
