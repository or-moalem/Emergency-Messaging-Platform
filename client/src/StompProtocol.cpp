
#include "../include/StompProtocol.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <ctime>
#include <iomanip>
#include <fstream>

using std::string;
using std::vector;
using std::cout;
using std::endl;


StompProtocol::StompProtocol()
    : connectionHandler(nullptr),
      stateMutex(),
      isLoggedIn(false),
      nextReceiptId(0),
      nextSubscriptionId(0),
      currentUsername(""),
      dataMutex(),
      channelToSubId(),
      subIdToChannel(),
      receiptIdToMsg(),
      userChannelEvents() {}


bool StompProtocol::connect(const std::string& host, short port, 
                           const std::string& username, const std::string& password) {
    std::lock_guard<std::mutex> lock(stateMutex);
    
    if(isLoggedIn) {
        cout << "Already logged in" << endl;
        return false;
    }
    
    connectionHandler = std::unique_ptr<ConnectionHandler>(new ConnectionHandler(host, port));
    if(!connectionHandler->connect()) {
        cout << "Could not connect to server" << endl;
        connectionHandler.reset();       
        return false;
    }
    
    string frame = createConnectFrame(username, password);
    if(!connectionHandler->sendFrameAscii(frame, '\0')) {
        cout << "Failed to send CONNECT frame" << endl;
        connectionHandler.reset();
        connectionHandler.reset();
        return false;
    }
    
    currentUsername = username;
    return true;
}

void StompProtocol::disconnect() {
    channelToSubId.clear();
    subIdToChannel.clear();
    if (connectionHandler) {
        connectionHandler->close();
        connectionHandler = nullptr;
    }
    isLoggedIn = false;
    }

vector<string> StompProtocol::processInput(const string& input) {
    vector<string> frames;
    vector<string> parts = split(input, ' ');
    if(parts.empty()) return frames;
    
    const string& command = parts[0];

    if(command == "login") {

        if(isLoggedIn) {
        cout << "The client is already logged in, log out before trying again" << endl;
        return frames;
        }

        if(parts.size() < 4) {
            std::cout << "Invalid login command. Usage: login {host:port} {username} {password}" << std::endl;
            return frames;
        }

        std::string hostPort = parts[1];
        std::string username = parts[2];
        std::string password = parts[3];
        
        std::string host;
        short port;
        if(!parseHostPort(hostPort, host, port)) {
            std::cout << "Invalid host:port format" << std::endl;
            return frames;
        }
        
        if(!connect(host, port, username, password)) {
            std::cout << "Could not connect to server" << std::endl;
            return frames;
        }
        return frames;
    }
    
    // All commands below require an active connection
    if(!isConnected()) {
        std::cout << "Not connected to server. Please login first." << std::endl;
        return frames;
    }

    // Handle channel-related commands
    if(command == "join") {
       if(parts.size() < 2) {
        std::cout << "Invalid join command. Usage: join {channel}" << std::endl;
        return frames;
    }

    std::lock_guard<std::mutex> lock(dataMutex);
    
    string channel = parts[1];

    try {
        if(channelToSubId.count(channel) == 0) {
            int subId = nextSubscriptionId.fetch_add(1);
            channelToSubId[channel] = subId;
            subIdToChannel[subId] = channel;
            string receipt = std::to_string(nextReceiptId.fetch_add(1));
            receiptIdToMsg[receipt] = "Joined channel " + channel;
            
            std::string subscribeFrame = createSubscribeFrame(channel);
            frames.push_back(subscribeFrame);
        } else {
            std::cout << "Already subscribed to channel: " << channel << std::endl;
        }
    } catch (const std::exception& e) {
        std::cout << "Exception in join handler: " << e.what() << std::endl;
    }
}

    else if(command == "exit") {
        if(parts.size() < 2) {
            std::cout << "Invalid exit command. Usage: exit {channel}" << std::endl;
            return frames;
        }

        std::lock_guard<std::mutex> lock(dataMutex);
        string channel = parts[1];
        if(channelToSubId.count(channel) > 0) {
            int subId = channelToSubId[channel];
            frames.push_back(createUnsubscribeFrame(subId));
            string receipt = std::to_string(nextReceiptId++);
            receiptIdToMsg[receipt] = "Exited channel " + channel;
            channelToSubId.erase(channel);
            subIdToChannel.erase(subId);
        }else {
            std::cout << "Not subscribed to channel: " << channel << std::endl;
        }
    }
    
    else if(command == "report") {
        if(parts.size() < 2) {
            std::cout << "Invalid report command. Usage: report {json_path}" << std::endl;
            return frames;
        }

        try {
            names_and_events eventsData = parseEventsFile(parts[1]);

            std::lock_guard<std::mutex> lock(dataMutex);
    
            for(const Event& event : eventsData.events) {
            std::string channel = event.get_channel_name();
            saveEventForUser(channel, currentUsername, event);
            std::string frame = createSendFrame(channel, formatEventMessage(event));
            frames.push_back(frame);
            }
        }
    catch(const std::exception& e) {
        cout << "Error processing report file: " << e.what() << endl;
        cout << "Make sure the file exists and is in the correct path" << endl;
    }
    }
    
    else if(command == "summary") {
        if(parts.size() < 4) {
            std::cout << "Invalid summary command. Usage: summary {channel} {user} {file}" << std::endl;
            return frames;
        }

        std::lock_guard<std::mutex> lock(dataMutex);
        writeEventSummary(parts[1], parts[2], parts[3]);
    }
    else if(command == "logout") {
        string frame = createDisconnectFrame();
        frames.push_back(frame);
    }
    else {
        std::cout << "Unknown command: " << command << std::endl;
    }

    return frames;
}

void StompProtocol::processResponse(const string& response) {
    vector<string> lines = split(response, '\n');
    if(lines.empty()) return;
    
    if(lines[0] == "CONNECTED") {
        std::lock_guard<std::mutex> lock(stateMutex);
        isLoggedIn = true;
        cout << "Login successful" << endl;
    }
    else if(lines[0] == "ERROR") {
        cout << "Error: " << getErrorMessage(lines) << endl;
        disconnect();
        return;
    }
    else if(lines[0] == "RECEIPT") {
        string receiptId = getHeader("receipt-id", lines);
        
        string msg;
        {
            std::lock_guard<std::mutex> lock(dataMutex);
            if(receiptIdToMsg.count(receiptId) > 0) {
                msg = receiptIdToMsg[receiptId];
                receiptIdToMsg.erase(receiptId);
            }
        }
        
        if(!msg.empty()) {
            if(msg == "disconnect") {
                disconnect();
            }
            cout << msg << endl;
        }
    }
    else if(lines[0] == "MESSAGE") {
        string channel = getHeader("destination", lines);

        if(channel[0] == '/') {
            channel = channel.substr(1);
        }
        string messageBody;

        bool headerEnded = false;
        
        for(const string& line : lines) {
            if(!headerEnded && line.empty()) {
                headerEnded = true;
                continue;
            }
            if(headerEnded) {
                messageBody += line + "\n";
            }
        }
        
        if(messageBody.empty()) {
            return;
        }
        
        if(!messageBody.empty()) {
            try {
                Event event(messageBody);

                std::string user = event.getEventOwnerUser();
                if(!user.empty() && currentUsername != user) {
                    std::lock_guard<std::mutex> lock(dataMutex);
                    saveEventForUser(channel, user, event);
                }
            }
            catch(const std::exception& e) {
                std::cout << "Error processing event: " << e.what() << std::endl;
            }
        } 
    }
}

string StompProtocol::createConnectFrame(const string& username, const string& password) {
    std::stringstream frame;
    frame << "CONNECT\n"
          << "accept-version:1.2\n"
          << "host:stomp.cs.bgu.ac.il\n"
          << "login:" << username << "\n"
          << "passcode:" << password << "\n\n";
    return frame.str();
}

std::string StompProtocol::createSubscribeFrame(const std::string& channel) {
    std::stringstream frame;
    std::string receiptId = std::to_string(nextReceiptId);
    receiptIdToMsg[receiptId] = "Joined channel " + channel;

    frame << "SUBSCRIBE\n"
          << "destination:" << channel << "\n"  // Added leading '/' as per examples
          << "id:" << channelToSubId[channel] << "\n"
          << "receipt:" << receiptId << "\n\n";
              
    return frame.str();
}

string StompProtocol::createUnsubscribeFrame(int subscriptionId) {
    std::stringstream frame;
    frame << "UNSUBSCRIBE\n"
          << "id:" << subscriptionId << "\n"
          << "receipt:" << nextReceiptId << "\n\n";
    return frame.str();
}

string StompProtocol::formatEventMessage(const Event& event) const {
    std::stringstream ss;
    ss << "user: " << currentUsername << "\n"
       << "city: " << event.get_city() << "\n"
       << "event name: " << event.get_name() << "\n"
       << "date time: " << event.get_date_time() << "\n"
       << "general information:\n";
    
    //Add general information fields
    const auto& info = event.get_general_information();
    for(const auto& [key, value] : info) {
        ss << "  " << key << ": " << value << "\n";
    }
    
    ss << "description:\n" << event.get_description() << "\n";
    return ss.str();
}

string StompProtocol::createSendFrame(const string& destination, const string& body) {
    std::stringstream frame;
    frame << "SEND\n"
          << "destination:/" << destination << "\n\n"
          << body;
    return frame.str();
}

string StompProtocol::createDisconnectFrame() {
    std::stringstream frame;
    std::string receiptId = std::to_string(nextReceiptId);
    
    // Save the pending message for this receipt
    {
        std::lock_guard<std::mutex> lock(dataMutex);
        receiptIdToMsg[receiptId] = "disconnect";  
    }
    
    frame << "DISCONNECT\n"
          << "receipt:" << receiptId << "\n\n";
          
    return frame.str();
}

bool StompProtocol::send(const string& frame) {
    
    if (!connectionHandler) {
        return false;
    }
    
    bool result = connectionHandler->sendFrameAscii(frame, '\0');
    return result;
}

bool StompProtocol::receiveFrame(string& frame) {
    return connectionHandler && connectionHandler->getFrameAscii(frame, '\0');
}

//Helper methods
vector<string> StompProtocol::split(const string& s, char delimiter) const {
    vector<string> tokens;
    std::stringstream ss(s);
    string token;
    while(std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

string StompProtocol::getHeader(const string& header, const vector<string>& lines) const {
    for(const string& line : lines) {
        size_t pos = line.find(':');
        if(pos != string::npos) {
            if(line.substr(0, pos) == header) {
                return line.substr(pos + 1);
            }
        }
    }
    return "";
}

void StompProtocol::saveEventForUser(const string& channel, const string& user, const Event& event) {
    string key = channel + "_" + user;
    userChannelEvents[key].push_back(event);
}

void StompProtocol::writeEventSummary(const string& channel, const string& user, const string& filename) {

    std::vector<Event> events;
    string key = channel + "_" + user;
    int activeEvents = 0;
    int forcesArrived = 0;
   
    if(userChannelEvents.count(key) > 0) {
        events = userChannelEvents[key];
        std::sort(events.begin(), events.end(), 
            [](const Event& a, const Event& b) {
                if(a.get_date_time() != b.get_date_time())
                    return a.get_date_time() < b.get_date_time();
                return a.get_name() < b.get_name();
            });

        for(const Event& event : events) {
        const auto& info = event.get_general_information();
        if(info.count("active") > 0 && info.at("active") == "true") 
            activeEvents++;
        if(info.count("forces_arrival_at_scene") > 0 && info.at("forces_arrival_at_scene") == "true")
            forcesArrived++;
        }
    }
    
    std::ofstream file(filename, std::ios::trunc);
    if(!file.is_open()) {
        std::cout << "Error: Could not open file for writing: " << filename << endl;
        return;
    }

    file << "Channel " << channel << endl;
    file << "Stats:" << endl;
    // Count statistics
    int totalEvents = events.size();
    file << "Total: " << totalEvents << endl;
    file << "active: " << activeEvents << endl;
    file << "forces arrival at scene: " << forcesArrived << endl << endl;
    
  
    // Write event reports
    if(!events.empty()) {
        file << "Event Reports:" << endl;
        for(size_t i = 0; i < events.size(); i++) {
            const Event& event = events[i];
            file << "Report_" << (i+1) << ":" << endl;
            file << "city: " << event.get_city() << endl;
            file << "date time: " << formatDateTime(event.get_date_time()) << endl;
            file << "event name: " << event.get_name() << endl;
            
            string desc = event.get_description();
            if(desc.length() > 27) {
                desc = desc.substr(0, 27) + "...";
            }
            file << "summary: " << desc << endl << endl;
        }
    }

    file.close();

}

string StompProtocol::formatDateTime(int epochTime) const {
    time_t time = static_cast<time_t>(epochTime);
    struct tm* timeinfo = localtime(&time);
    char dateStr[80];
    strftime(dateStr, sizeof(dateStr), "%d/%m/%y %H:%M", timeinfo);
    return string(dateStr);
}

bool StompProtocol::isConnected() const {
    return connectionHandler != nullptr && connectionHandler->isConnected();
}

bool StompProtocol::parseHostPort(const std::string& hostPort, std::string& host, short& port) {

    size_t colonPos = hostPort.find(':');
    if(colonPos == std::string::npos) return false;
    
    try {
        host = hostPort.substr(0, colonPos);
        port = std::stoi(hostPort.substr(colonPos + 1));
        return true;
    } catch(...) {
        return false;
    }
}

string StompProtocol::getErrorMessage(const std::vector<string>& lines) const {
    for (const string& line : lines) {
        if (line.find("message:") == 0) {
            return line.substr(8); // Skip "message:"
        }
    }
    // If no message header found, join all lines after ERROR
    string message;
    bool first = true;
    for (const string& line : lines) {
        if (first) {
            first = false;
            continue;
        }
        message += line + "\n";
    }
    return message;
}
