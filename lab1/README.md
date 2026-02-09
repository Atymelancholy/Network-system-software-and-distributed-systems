### Знакомство с программированием сокетов

Перед началом работы необходимо запустить сервер:

Windows:

java -cp target\classes org.example.TcpFileServer

или с параметрами

java -cp target\classes org.example.TcpFileServer 50505 server_storage

Linux:

java -cp out org.example.TcpFileServer

или с параметрами

java -cp out org.example.TcpFileServer 50505 server_storage

1. Проверка текстовых команд

 1.1 TIME

Windows:

java -cp <путь_к_classes> org.example.TcpFileClient <_host> <_port> time

java -cp target\classes org.example.TcpFileClient 127.0.0.1 50505 time

Linux:

java -cp out org.example.TcpFileClient 127.0.0.1 50505 time

 1.2 ECHO

Windows:

java -cp <путь_к_classes> org.example.TcpFileClient <_host> <_port> echo <текст>

java -cp target\classes org.example.TcpFileClient 127.0.0.1 50505 echo hello world

Linux:

java -cp out org.example.TcpFileClient 127.0.0.1 50505 echo hello

 1.3 CLOSE

Windows:

java -cp <путь_к_classes> org.example.TcpFileClient <_host> <_port> close

java -cp target\classes org.example.TcpFileClient 127.0.0.1 50505 close

Linux:

java -cp out org.example.TcpFileClient 127.0.0.1 50505 close

2. Загрузка файла на сервер (UPLOAD)

Windows:

java -cp <путь_к_classes> org.example.TcpFileClient <_host> <_port> upload <локальный_файл> <имя_на_сервере>

java -cp target\classes org.example.TcpFileClient 127.0.0.1 50505 upload C:\temp\big.bin big.bin

Linux:

java -cp out org.example.TcpFileClient 127.0.0.1 50505 upload ./big.bin big.bin

3. Скачивание файла с сервера (DOWNLOAD)

Windows:

java -cp <путь_к_classes> org.example.TcpFileClient <_host> <_port> download <имя_на_сервере> <локальный_файл>

java -cp target\classes org.example.TcpFileClient 127.0.0.1 50505 download big.bin C:\temp\big_download.bin

Linux:

java -cp out org.example.TcpFileClient 127.0.0.1 50505 download big.bin ./big.bin

4. Проверка открытого порта сервера

Windows:

netstat -ano | findstr :<_port>

netstat -ano | findstr :50505

Linux:

ss -lntp | grep <_port>

ss -lntp | grep 50505

Общая команда:

nmap -p <port> <_host>

nmap -p 50505 127.0.0.1
