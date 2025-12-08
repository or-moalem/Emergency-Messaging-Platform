#include <iostream>
#include "../include/keyboardInput.h"
#include <sstream>

KeyboardInput::KeyboardInput(StompProtocol& protocol)
    : protocol(protocol), inputThread(), shouldStop(false) {

    }
    
void KeyboardInput::run() {

    while(!shouldStop) {
        // Read a line from stdin
        const short bufsize = 1024;
        char buf[bufsize];
        std::cin.getline(buf, bufsize);
        
        if(shouldStop) {
            break;
        }

        std::string line(buf);
        if(line.empty()) continue;

        // Pass the input to the protocol for processing
               std::vector<std::string> frames = protocol.processInput(line);
        
        // Send any generated frames
        for(const std::string& frame : frames) {
            if(!frame.empty()) {
                if(!protocol.send(frame)) {
                    std::cout << "Error sending frame" << std::endl;
                    protocol.disconnect();
                    break;
                }
            }
        }
    }
}

void KeyboardInput::start() {
    inputThread = std::thread(&KeyboardInput::run, this);
}

void KeyboardInput::stop() {
    shouldStop = true;
    pthread_kill(inputThread.native_handle(), SIGINT);
    
    // Wait for the thread to finish
    if (inputThread.joinable()) {
        inputThread.join();
    }
}

KeyboardInput::~KeyboardInput() {
    stop();
}