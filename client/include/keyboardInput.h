#pragma once
#include "../include/StompProtocol.h"
#include <thread>
#include <atomic>
#include <string>

class KeyboardInput {
private:
    StompProtocol& protocol;
    std::thread inputThread;
    std::atomic<bool> shouldStop;
    void run();


public:
    explicit KeyboardInput(StompProtocol& protocol);
    void start();
    void stop();
    ~KeyboardInput();
};
