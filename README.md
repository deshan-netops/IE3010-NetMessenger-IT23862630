# NetMessenger

A multi-client TCP network messaging application developed for the **IE3010 – Network Programming** module at the **Sri Lanka Institute of Information Technology (SLIIT)**.

The application provides real-time messaging, private messaging, room-based communication, and file transfer between multiple TCP clients. The server uses a **POSIX thread-per-client concurrency model** to support simultaneous connections.

---

## 📌 Project Overview

**NetMessenger** is a client-server network messaging system implemented using the C programming language and TCP sockets.

The server can handle multiple connected clients simultaneously. Each client communicates with the server using a custom text-based application protocol.

### Main Features

* 👤 User registration
* 👥 List connected users
* 📢 Broadcast messaging
* 💬 Private messaging
* 🏠 Chat rooms
* 📩 Room-based messaging
* 📁 File transfer
* 🔌 Client connection/disconnection handling
* 📝 Server event logging
* ⚠️ Protocol error handling
* 🧵 Multi-client concurrency using POSIX threads

---

## 🏗️ System Architecture

```text
                  ┌─────────────────────┐
                  │     TCP Server      │
                  │     Port: 8630      │
                  └──────────┬──────────┘
                             │
                ┌────────────┼────────────┐
                │            │            │
                ▼            ▼            ▼
          ┌──────────┐ ┌──────────┐ ┌──────────┐
          │ Client 1 │ │ Client 2 │ │ Client 3 │
          └──────────┘ └──────────┘ └──────────┘
                │            │            │
                └────────────┼────────────┘
                             │
                      POSIX Threads
```

The server creates a dedicated POSIX thread for each connected client. Each thread handles client communication using blocking `recv()` calls. This model provides simple per-client state management while allowing multiple clients to communicate concurrently.

The implementation supports up to **64 simultaneous clients**.

---

## 🧵 Concurrency Model

The server uses a **one POSIX thread per client** architecture.

```text
Client Connection
       │
       ▼
accept()
       │
       ▼
pthread_create()
       │
       ▼
Client Handler Thread
       │
       ├── REGISTER
       ├── LIST
       ├── BCAST
       ├── PMSG
       ├── JOIN
       ├── LEAVE
       ├── ROOM
       ├── RMSG
       ├── SENDFILE
       └── QUIT
```

Threads share the same process address space, allowing the server to access other clients' sockets directly when delivering broadcasts, private messages, room messages, and files.

---

## 📂 Project Structure

```text
NetMessenger/
│
├── server_2630.c
├── client_2630.c
├── Makefile_2630
├── note.txt
├── netmsg_IT23862630.log
│
├── storage/
│   └── IT23862630/
│       └── <sender>/
│           └── received files
│
└── README.md
```

---

## 🔢 Personalization

The project uses values derived from registration number:

| Item                | Value                                         |
| ------------------- | --------------------------------------------- |
| Registration Number | `IT23862630`                                  |
| Numeric Part        | `23862630`                                    |
| Last Four Digits    | `2630`                                        |
| Server Port         | `8630`                                        |
| Node ID             | `NID:8626`                                    |
| Log File            | `netmsg_IT23862630.log`                       |
| Storage Path        | `/home/deshan-techoops/netmessenger/note.txt` |

The server port is calculated as:

```text
6000 + last four digits
6000 + 2630
= 8630
```

The Node ID is derived from digits 3–6 of the numeric portion of the registration number.

---

## 📡 Communication Protocol

The application implements the following commands.

| Command                                   | Description                |
| ----------------------------------------- | -------------------------- |
| `REGISTER <username>`                     | Register a new user        |
| `LIST`                                    | Display registered users   |
| `BCAST <message>`                         | Send a broadcast message   |
| `PMSG <message>`                          | Send a private message     |
| `JOIN <room>`                             | Join a chat room           |
| `LEAVE <room>`                            | Leave a chat room          |
| `ROOM`                                    | Display joined rooms       |
| `RMSG <room> <message>`                   | Send a room message        |
| `SENDFILE <target> <filename> <filesize>` | Transfer a file            |
| `QUIT`                                    | Disconnect from the server |

The implementation also provides protocol responses such as `OK REGISTERED`, `OK SENT`, `ERR 001 USERNAME_TAKEN`, `ERR 002 USER_NOT_FOUND`, and `ERR 007 BAD_SYNTAX`.

---

## 💬 Messaging

### Broadcast Messaging

A registered client can broadcast a message to all other registered clients.

```text
BCAST Hello everyone!
```

The sender receives:

```text
OK SENT
```

Other clients receive:

```text
MSG BCAST <sender> Hello everyone!
```

---

### Private Messaging

A client can send a private message to a specific user.

```text
PMSG kasun Hello!
```

Only the target user receives the message:

```text
MSG PRIV <sender> Hello!
```

---

## 🏠 Chat Rooms

Users can create/join rooms and communicate with other room members.

### Join a room

```text
JOIN lab
```

### Send a room message

```text
RMSG lab Hello room!
```

Only members of the room receive:

```text
MSG ROOM lab <sender> Hello room!
```

A user who is not a member of the room cannot send room messages.

---

## 📁 File Transfer

NetMessenger supports file transfers between users and rooms.

Example:

```text
SENDFILE kasun note.txt 1024
```

The protocol sends the file notification followed immediately by the specified number of raw bytes.

```text
MSG FILE <sender> <filename> <filesize>
```

The maximum supported file size is **10 MB**. Zero-byte files are allowed.

---

## 📝 Logging

The server maintains a timestamped log file:

```text
netmsg_IT23862630.log
```

The log records important events including:

* Client connections
* User registration
* Broadcast messages
* Private messages
* Room activity
* File transfers
* Client disconnections

---

## ⚙️ Requirements

### Operating System

Linux is recommended.

### Required Software

* GCC
* GNU Make
* POSIX Threads
* Linux TCP/IP socket support

---

## 🔨 Compilation

Clone the repository:

```bash
git clone https://github.com/deshan-netops/IE3010-NetMessenger-IT23862630.git
cd IE3010-NetMessenger-IT23862630
```

Compile the server:

```bash
gcc -Wall -Wextra -pthread server_2630.c -o server
```

Compile the client:

```bash
gcc -Wall -Wextra -pthread client_2630.c -o client
```

Or use the provided Makefile:

```bash
make -f Makefile_2630
```

---

## ▶️ Running the Application

### 1. Start the Server

```bash
./server
```

The server listens on:

```text
Port: 8630
```

Check that the server is listening:

```bash
ss -tlnp | grep 8630
```

---

### 2. Start a Client

From another terminal:

```bash
./client 127.0.0.1 8630
```

Multiple clients can be started from separate terminals.

Example:

```text
Terminal 1 → Client 1
Terminal 2 → Client 2
Terminal 3 → Client 3
Terminal 4 → Client 4
Terminal 5 → Client 5
```

The implementation was tested with five simultaneous clients.

## 🧪 Testing

The application was tested for:

* Multiple simultaneous clients
* User registration
* Duplicate usernames
* Invalid usernames
* Broadcast messaging
* Private messaging
* Room creation and messaging
* Room membership validation
* File transfer
* Large file rejection
* Unsafe file names
* Client disconnection
* Unexpected client termination
* Unknown commands
* Invalid syntax
* Long input lines
* Server stability after client failure

The tests confirmed that the server continues running correctly when clients disconnect unexpectedly and that file transfers preserve the original file contents.

## 🔐 Protocol and Application Limits

| Feature                  | Limit          |
| ------------------------ | -------------- |
| Maximum clients          | 64             |
| Maximum file size        | 10 MB          |
| Maximum username length  | 31 characters  |
| Maximum room name length | 31 characters  |
| Maximum rooms per client | 8              |
| Maximum file name length | 100 characters |
| Maximum input line       | 8192 bytes     |
| Network                  | IPv4           |
| Transport                | TCP            |

The current implementation uses plain TCP without encryption and was tested on a single machine using the loopback address.


## 🛠️ Technologies Used

* **C**
* **TCP/IP**
* **POSIX Sockets**
* **POSIX Threads (pthreads)**
* **Linux**
* **GCC**
* **Make**
* **File I/O**
* **Concurrent Network Programming**

## 📚 Academic Context

This project was developed as part of:

**IE3010 – Network Programming**
**BSc (Hons) Information Technology**
**Sri Lanka Institute of Information Technology (SLIIT)**


## 👨‍💻 Author

Chathuranga H.A.D

