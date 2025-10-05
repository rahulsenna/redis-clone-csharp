using System.Net;
using System.Net.Sockets;
using System.Text;


const int DEFAULT_BUFFER_SIZE = 1024;

TcpListener server = new(IPAddress.Any, 6379);
server.Start();

while (true)
{
  Socket socket = await server.AcceptSocketAsync();
  // _ = HandleClient(socket);
  _ = Task.Run(() => HandleClient(socket));

}

async Task HandleClient(Socket socket)
{
  using var _ = socket; // Auto dispose at end of function 
  byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
  while (true)
  {
    int bytesRead = await socket.ReceiveAsync(buffer);
    if (bytesRead <= 0) break;

    string str = Encoding.UTF8.GetString(buffer, 0, bytesRead);
    var commands = str.Split("\r\n", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Where((_, idx) => idx % 2 == 0).ToArray();
    string command = commands[1];

    if (command == "PING")
      await socket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"));
  }

}
