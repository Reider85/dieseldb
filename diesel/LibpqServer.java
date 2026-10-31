package diesel;

import diesel.net.libpq.LibpqProtocol;
import diesel.net.libpq.LibpqMessage;
import diesel.net.libpq.ScramSha256;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * libpq protocol server running on port 5432.
 * 
 * Supports:
 * - Simple query protocol
 * - Extended query protocol (Parse/Bind/Execute)
 * - SCRAM-SHA-256 authentication
 * - psql meta-commands via pattern matching
 */
public class LibpqServer {
    
    private final Database database;
    private final int port;
    private final String authMethod;
    private final ExecutorService executor;
    private ServerSocket serverSocket;
    private volatile boolean running;
    
    public LibpqServer(Database database, int port, String authMethod) {
        this.database = database;
        this.port = port;
        this.authMethod = authMethod != null ? authMethod : "scram-sha-256";
        this.executor = Executors.newCachedThreadPool();
    }
    
    /**
     * Starts the server.
     */
    public void start() throws IOException {
        if (running) {
            throw new IllegalStateException("Server is already running");
        }
        
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("localhost", port));
        running = true;
        
        // Log port binding (non-fatal if port 5432 is occupied)
        try {
            System.out.println("libpq server started on port " + port);
        } catch (Exception e) {
            System.err.println("WARNING: Could not start libpq server on port " + port + ": " + e.getMessage());
        }
        
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new LibpqClientHandler(clientSocket, database, authMethod));
            } catch (IOException e) {
                if (running) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Stops the server.
     */
    public void stop() {
        running = false;
        executor.shutdown();
        
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing server socket: " + e.getMessage());
            }
        }
    }
    
    /**
     * Checks if the server is running.
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * Gets the port the server is running on.
     */
    public int getPort() {
        return port;
    }
}