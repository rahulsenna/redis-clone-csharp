using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;


const int DEFAULT_BUFFER_SIZE = 1024;

TcpListener server = new(IPAddress.Any, 6379);
server.Start();
ConcurrentDictionary<string, RedisValue> _db = [];
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
    var query = str.Split("\r\n", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Where((_, idx) => idx % 2 == 0).ToArray();
    string command = query[1];

    if (command == "PING")
      await socket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"));

    else if (command == "ECHO")
      await socket.SendAsync(Encoding.UTF8.GetBytes($"+{query[2]}\r\n"));

    else if (command == "SET")
    {
      long? ExpireAt = null;
      if (query.Length > 5 && query[4].Equals("px", StringComparison.OrdinalIgnoreCase))
        ExpireAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + Convert.ToInt64(query[5]);

      string key = query[2];
      _db[key] = new RedisValue(RedisType.String, query[3], ExpireAt);
      await socket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
    }
    else if (command == "GET")
    {
      string key = query[2];

      if (!_db.TryGetValue(key, out var value) || value.IsExpired())
        await socket.SendAsync(Encoding.UTF8.GetBytes("$-1\r\n"));
      else
        await socket.SendAsync(Encoding.UTF8.GetBytes($"+{value.Data as string}\r\n"));
    }
  }
}

public enum RedisType { None, String }

public sealed record RedisValue(
  RedisType Type,
  object? Data,
  long? ExpireAtMs
)
{
  public bool IsExpired() => ExpireAtMs is long && ExpireAtMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
};