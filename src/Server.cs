using System.Net;
using System.Net.Sockets;

Console.WriteLine("Logs from your program will appear here!");

TcpListener server = new(IPAddress.Any, 6379);
server.Start();
server.AcceptSocket(); // wait for client
