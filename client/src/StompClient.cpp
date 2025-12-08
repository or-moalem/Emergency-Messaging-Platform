#include "../include/StompProtocol.h"
#include <thread>
#include <iostream>
#include <sstream>
#include <utility>
#include "../include/keyboardInput.h"


int main(int argc, char *argv[]) {
    StompProtocol protocol;
    
    KeyboardInput keyboardInput(protocol);
   
    keyboardInput.start();
    
    while(true) {  
            if(protocol.isConnected()) {
                std::string response;
    
                if(protocol.receiveFrame(response)) {
                    if(!response.empty()) {
                        protocol.processResponse(response);
                    }
                }
            }
            else {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
           
        keyboardInput.stop();

    return 0; 
}