import socket

from data_loader import load_db_config

def shudtd():
    cfg = load_db_config()
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((cfg['host'], 8080))

    request = "GET /shutd HTTP/1.1\r\nHost: localhost\r\n\r\n"
    client.sendall(request.encode())

    response = client.recv(1024)
    print(response.decode())

    client.close()
