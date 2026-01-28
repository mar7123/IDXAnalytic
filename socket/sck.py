import socket
import os

HOST = ''
PORT = 8080

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, PORT))
server_socket.listen(5)

print(f"Listening on port {PORT}...")

while True:
    client_socket, addr = server_socket.accept()
    print(f"Connection from {addr}")
    request = client_socket.recv(1024).decode()
    print(request)
    strs = request.split()
    if len(strs) >= 2 and strs[0] == "GET" and strs[1] == "/shutd":
        os.system("shutdown /s /t 0")
    response = (
        "HTTP/1.1 200 OK\r\n"
        "\r\n"
        ""
    )
    client_socket.sendall(response.encode())
    client_socket.close()
