# 📡 Emergency Messaging Platform  
### Java STOMP Server + C++ Asynchronous Client

A real-time messaging system implementing the **STOMP 1.2 protocol**.  
Includes a **Java server** (TPC/Reactor) and a **multithreaded C++ client** supporting asynchronous pub/sub communication.

---

## 🧰 Tech Stack

### **Server (Java)**
- Java  
- Maven  
- Reactor / Thread-Per-Client  
- STOMP 1.2 over TCP  

### **Client (C++)**
- Modern C++  
- Boost.Asio  
- Multithreading (keyboard thread + network thread)  
- Mutex & condition_variable synchronization  

---

## 📂 Project Structure

   emergency-messaging-platform/
│
├── README.md <--
│
├── client/
│ ├── src/ <-- C++ source files
│ ├── include/ <-- headers
│ ├── data/ <-- sample event files
│ ├── bin/ <-- compiled executable (after make)
│ └── makefile
│
└── server/
├── src/ <-- Java server code
├── pom.xml <-- Maven config
├── manifest.txt
└── target/ <-- built JAR (optional)


---

## ✨ Features

### 🔸 **Java STOMP Server**
- Supports both **TPC** and **Reactor** modes  
- Routes STOMP frames between clients  
- Manages subscriptions, topics, receipts & error frames  

### 🔸 **C++ Asynchronous Client**
- Two-thread architecture:  
  - 🧑‍💻 **Keyboard thread** – parses user commands  
  - 🌐 **Socket thread** – receives STOMP frames from server  
- Full STOMP frame parser + encoder  
- Supports: SEND, SUBSCRIBE, UNSUBSCRIBE, CONNECT, DISCONNECT  
- Handles user event files & channel-based communication  

---

## ▶️ How to Run

### **1️⃣ Start the STOMP Server (Java + Maven)**

From inside the **server** folder:

```bash
mvn exec:java \
  -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" \
  -Dexec.args="7777 tpc"
