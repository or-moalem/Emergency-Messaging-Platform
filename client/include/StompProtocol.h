#pragma once

#include "../include/ConnectionHandler.h"
#include "../include/event.h"
#include <map>
#include <vector>
#include <string>
#include <mutex>
#include <atomic>
#include <memory>


class StompProtocol {
private:
    // Connection management
    std::shared_ptr<ConnectionHandler> connectionHandler;
    std::mutex stateMutex;
    
    // STOMP Protocol state
    std::atomic<bool> isLoggedIn{false};
    std::atomic<int> nextMessageId{0};
    std::atomic<int> nextReceiptId{0}; 
    std::atomic<int> nextSubscriptionId{0};
    std::string currentUsername;
    
    // Thread-safe data structures
    std::mutex dataMutex;
    std::map<std::string, int> channelToSubId;    // channel to subId
    std::map<int, std::string> subIdToChannel;    // subId to channel
    std::map<std::string, std::string> receiptIdToMsg;  // receiptId to pending message
    std::map<std::string, std::vector<Event>> userChannelEvents; // channel_user to events
    
    // Frame creation methods
    std::string createConnectFrame(const std::string& username, const std::string& password);
    std::string createSubscribeFrame(const std::string& channel);
    std::string createUnsubscribeFrame(int subscriptionId);
    std::string createSendFrame(const std::string& destination, const std::string& body);
    std::string createDisconnectFrame();
    
    // Helper methods
    std::vector<std::string> split(const std::string& s, char delimiter) const;
    std::string getHeader(const std::string& header, const std::vector<std::string>& lines) const;
    void saveEventForUser(const std::string& channel, const std::string& user, const Event& event);
    std::string formatDateTime(int epochTime) const;
    std::string formatEventMessage(const Event& event) const;
    bool parseHostPort(const std::string& hostPort, std::string& host, short& port);
    std::string getErrorMessage(const std::vector<std::string>& lines) const;


public:
    StompProtocol();
    ~StompProtocol() = default;
    
    // Connection management
    bool connect(const std::string& host, short port, 
                const std::string& username, const std::string& password);
    void disconnect();
    bool isConnected() const;
    
    // Main protocol operations
    std::vector<std::string> processInput(const std::string& input);
    void processResponse(const std::string& response);
    bool send(const std::string& frame);
    bool receiveFrame(std::string& frame);
    
    // Event handling
    void writeEventSummary(const std::string& channel, const std::string& user, const std::string& filename);
};
