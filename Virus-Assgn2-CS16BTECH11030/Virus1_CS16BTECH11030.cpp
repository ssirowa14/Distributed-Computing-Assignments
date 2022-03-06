#include <bits/stdc++.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <semaphore.h>
#include <sys/types.h>
#include <error.h>
#include <iostream>
#include <fcntl.h>
typedef long long ll;
#define BASE 11000      //port are assigned starting from 11000
using namespace std;
using namespace chrono;

int N, Ir, Ib, root=-1;
float Wr, Wb, lambdaBlue, lambdaRed, lambdaSnd, p, q;
FILE *ofile;
unordered_map <int, vector<int>> adjList, spanning;
unordered_map <int,int> par;
vector <int> nodeID;
system_clock::time_point startTime, endTime;

struct Packet{          //Packet sent over network
    ll from, color, tokenColor;
};

class Node{             //class for process
	ll port, id, parent;          //port for server to receive requests
    atomic <ll> state, childLeft, tokenColor;   //state 0:White, 1:red, 2:blue ; color 0:white, 1:black
                                                //childLeft is number of child process yet to send terminate msg
    atomic <bool> exitSend, exitRecv;           //flags to exit send and receive thread

public:
    atomic <ll> controlMsgCount, basicMsgCount;

    Node(int i){        //constructor
        state = 0;
        tokenColor = 0;
        id = i;
        basicMsgCount = controlMsgCount = 0;
        exitSend = false;
        exitRecv = false;
        port = BASE+i;
        if(par.find(id) != par.end()){
            parent = par[id];
        }
        else{
            parent = -1;
        }
        childLeft = spanning[id].size();
    }

    Node(const Node &copy){     //copy constructor
        state = 0;
        tokenColor = 0;
        id = copy.id;
        controlMsgCount = 0;
        exitSend = false;
        exitRecv = false;
        port = copy.port;
        if(par.find(id) != par.end()){
            parent = par[id];
        }
        else{
            parent = -1;
        }
        childLeft = spanning[id].size();
    }

    double ran_exp(float lambda){       //function to return exponential distribution time
        default_random_engine gen(time(0));
        exponential_distribution <double> dist(1.0/lambda);
        return dist(gen);
    }

    //function to send Mesages
    int sendRequest(ll from, ll color, ll serverPort){
        //creating socket
        int sock;
        struct sockaddr_in serv_addr;
        if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
            printf("Error : sendRequest : socket : %lld,%lld,%lld : %s\n", (ll)port-BASE, (ll)serverPort-BASE, color, strerror(errno));
            return -1; 
        }
        serv_addr.sin_family = AF_INET; 
        serv_addr.sin_port = htons(serverPort); 
        if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0){
            printf("Error : sendRequest : inet_pton : %lld,%lld,%lld : %s\n", (ll)port-BASE, (ll)serverPort-BASE, color, strerror(errno));
            close(sock);
            return -1; 
        }
        if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
            printf("Error : sendRequest : connect : %lld,%lld,%lld : %s\n", (ll)id, (ll)serverPort-BASE, color, strerror(errno));
            close(sock);
            return -1; 
        }
        Packet buff;
        buff.from = from;                   //sender
        buff.color = color;                 //msg type
        buff.tokenColor = tokenColor;       //process color/token it receives from children
        ll sleepForTime = ceil(ran_exp(lambdaSnd)+1);
        this_thread::sleep_for(chrono::microseconds(sleepForTime));
        while(send(sock, &buff, sizeof(buff), 0) < 0);      //send request
        close(sock);
        return 1;
    }

    //function to spread infection and antidote
    void initialize(){
        vector <int> temp;
        for(int i=1; i<N; i++){
            temp.push_back(nodeID[i]);
        }
        shuffle(temp.begin(), temp.end(), default_random_engine(time(0)));          //shuffle node ID
        ll sleepForTime = ceil(ran_exp(Wr)+1);                                      //red injection waiting time
        this_thread::sleep_for(chrono::milliseconds(sleepForTime));
        int itr = 0;
        tokenColor = 1;                 //process becomes black
        while(itr < Ir){                //red Injection
            while(sendRequest(id, 1, temp[itr]+BASE) < 0);
            itr++;
        }
        sleepForTime = ceil(ran_exp(Wb)+1);                                         //blue injection waiting time
        this_thread::sleep_for(chrono::milliseconds(sleepForTime));
        startTime = system_clock::now();                                            //start time of algo
        while(itr < Ir+Ib-1){                                                       //blue injection
            while(sendRequest(id, 2, temp[itr]+BASE) < 0);
            itr++;
        }
        state = 2;                                                                  //colour is blue. Root is always blue.
    }

    //function to synchronize distributed clocks
    void sendMsg(){
        if(id == root){                         //initial red and blue injection done by root
            initialize();
        }
        while(state == 0);                  //loop till white
        time_t my_time;
        auto timeinfo = localtime(&my_time);
        while(!exitSend || (exitSend && id == root)){
            int color = state;
            shuffle(adjList[id].begin(), adjList[id].end(), default_random_engine(time(0)));
            int limit=ceil((color==1 ? p : q)*(double)adjList[id].size());      //take first q precent from shuffled adjList 
            limit = min((int)adjList[id].size(), limit);    
            for(int i=0; i<limit; i++){                                         //send colour msg
                if(color == 1 && adjList[id][i] == root){                       //don't send red msg to root
                    continue;
                }
                if(color == 1){                                                 //red msg
                    basicMsgCount++;
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld sends Red msg to Cell %d at time %d:%d:%d\n",id,adjList[id][i],timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld sends Red msg to Cell %d at time %d:%d:%d\n",id,adjList[id][i],timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    if(tokenColor == 0){
                        tokenColor = 1;                                         //black process now. Sending blackens a process
                    }
                }
                else{                                                           //blue msg
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld sends Blue msg to Cell %d at time %d:%d:%d\n",id,adjList[id][i],timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld sends Blue msg to Cell %d at time %d:%d:%d\n",id,adjList[id][i],timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                }
                while(sendRequest(id, color, adjList[id][i]+BASE) < 0);         //msg Sent
                if(color == 1){                                                 //sleep after msg sent
                    ll sleepForTime = ceil(ran_exp(lambdaRed)+1);
                    this_thread::sleep_for(chrono::milliseconds(sleepForTime));
                }
                if(color == 2){
                    ll sleepForTime = ceil(ran_exp(lambdaBlue)+1);
                    this_thread::sleep_for(chrono::milliseconds(sleepForTime));
                }
            }
            if(childLeft == 0 && id == root && exitSend == true){                   //attempt to detect  termination by root. 
                if(tokenColor == 1){    //repeat. Black root
                    for(int i=0; i<spanning[id].size(); i++){
                        while(sendRequest(id, 4, spanning[id][i]+BASE) < 0);
                    }
                    reset();
                }
                else if(state == 2){                //termination detected
                    endTime = system_clock::now();      //algo end time
                    break;
                }
            }
            if(state == 2 && childLeft == 0 && parent != -1){   //send terminate message
                while(sendRequest(id, 3, parent+BASE) < 0);
                parent = -1;                                    //prevent from sending multiple terminate msg
            }            
        }
        for(int i=0; i<spanning[id].size(); i++){               //send shutdown message
            while(sendRequest(id, 5, spanning[id][i]+BASE) < 0);
        }
        time (&my_time);
        timeinfo = localtime (&my_time);
        fprintf(ofile, "Cell %lld initiates termination at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
        printf("Cell %lld initiates termination at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
    }

    //function to reset variables on receipt of repeat message
    void reset(){
        tokenColor = 0;
        childLeft = spanning[id].size();
        if(par.find(id) != par.end()){
            parent = par[id];
        }
        else{
            parent = -1;
        }
    }

    //function to receive request. It implements server
    int recieve(){
        //create socket
        int server_id, opt=1, error;
        struct sockaddr_in address, clientAddr;
        time_t my_time;
        auto timeinfo = localtime(&my_time);
        if((server_id = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)) < 0){
            printf("Error : Receive : socket : %lld : %s\n", (ll)port-BASE, strerror(errno));
            return -1;
        }
        if (setsockopt(server_id, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)) < 0){
            printf("Error : Receive : setsockopt : %lld : %s\n", (ll)port-BASE, strerror(errno));
            close(server_id);
            return -1;
        } 
        address.sin_family = AF_INET; 
        address.sin_addr.s_addr = INADDR_ANY; 
        address.sin_port = htons(port);
        if (bind(server_id, (struct sockaddr *)&address, sizeof(address)) < 0){
            printf("Error : Receive : bind : %lld : %s\n", (ll)port-BASE, strerror(errno));
            close(server_id);
            return -1; 
        }
        //listen
        if(listen(server_id, 2*N) < 0){
            printf("Error : Receive : listen : %lld : %s\n", (ll)port-BASE, strerror(errno));
            close(server_id);
            return -1;
        }
        int len, sockid;
        Packet buff;
        //server is in listening on port
        while(!exitRecv){
            len=sizeof(clientAddr);
            sockid = accept(server_id,(struct sockaddr*)&clientAddr, (socklen_t*)&len);
            //Connection Established
            if(sockid < 0){
                continue;
            }
            //get request
            while(recv(sockid, &buff, sizeof(buff), 0) < 0);
            if(buff.color == 1){   //basic (red Msg)
                time (&my_time);
                timeinfo = localtime (&my_time);
                fprintf(ofile, "Cell %lld Receives Red msg from Cell %lld at time %d:%d:%d\n",id,buff.from,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                printf("Cell %lld Receives Red msg from Cell %lld childLeft = %lld, state = %lld, parent = %lld, tokenColor = %lld\n",id,buff.from,(ll)childLeft, (ll)state, (ll)parent, (ll)tokenColor);
                if(state != 1){
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld turns Red at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld turns Red at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    state = 1;
                }
            }
            else if(buff.color == 2){   //become passive (Blue msg)
                time (&my_time);
                timeinfo = localtime (&my_time);
                fprintf(ofile, "Cell %lld Receives Blue msg from Cell %lld at time %d:%d:%d\n",id,buff.from,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                printf("Cell %lld Receives Blue msg from Cell %lld childLeft = %lld, state = %lld, parent = %lld, tokenColor = %lld\n",id,buff.from,(ll)childLeft, (ll)state, (ll)parent, (ll)tokenColor);
                if(state != 2){
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld turns Blue at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld turns Blue at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    state = 2;
                }
            }
            else if(buff.color == 3){   //terminate received from child
                controlMsgCount++;
                if(buff.tokenColor == 1 && tokenColor == 0){        //in case black token received
                    tokenColor = 1;                                 //process becomes black
                }
                childLeft--;                                        //children remaining to send a terminate msg
                if(childLeft == 0 && id == root){                   //Attempt to detect termination by root
                    if(tokenColor == 1){    //repeat. Black root
                        for(int i=0; i<spanning[id].size(); i++){
                            while(sendRequest(id, 4, spanning[id][i]+BASE) < 0);
                        }
                        reset();
                    }
                    else{           //possibility of a termination. Sender thread of root will attempt for termination
                        exitSend = true;
                    }
                }
                printf("Cell %lld Receives Ack msg from Cell %lld childLeft = %lld, state = %lld, parent = %lld, tokenColor = %lld\n",id,buff.from,(ll)childLeft, (ll)state, (ll)parent, (ll)tokenColor);
            }
            else if(buff.color == 4){   //repeat
                controlMsgCount++;
                for(int i=0; i<spanning[id].size(); i++){
                    while(sendRequest(id, 4, spanning[id][i]+BASE) < 0);
                }
                reset();            //reset variables
                printf("Cell %lld Receives Repeat msg from Cell %lld childLeft = %lld, state = %lld, parent = %lld, tokenColor = %lld\n",id,buff.from,(ll)childLeft, (ll)state, (ll)parent, (ll)tokenColor);
            }
            else if(buff.color == 5){   //shut down
                exitSend = true;            //exit Send
                printf("Cell %lld Receives Shut Down msg from Cell %lld childLeft = %lld, state = %lld, parent = %lld, tokenColor = %lld\n",id,buff.from,(ll)childLeft, (ll)state, (ll)parent, (ll)tokenColor);
            }
            else{
                printf("ERROR msg recvd by %lld\n", id);
            }
            close(sockid);          //close socket
        }
        close(server_id);           //close listening socket
        printf("--out-%lld\n", id);
    }
    void setExitRecv(){
        exitRecv = true;
    }
};

//function to parse graph
void parseGraph(ifstream &ifile, unordered_map <int, vector<int>> &graph, bool flag=true){
    string parse;
    stringstream stream;
    while(!ifile.eof()){
        getline(ifile, parse);
        if(parse == ""){
            break;
        }
        stream.clear();
        stream << parse;
        int u, v;
        stream >> u;
        if(flag){
            nodeID.push_back(u);
        }
        while(stream >> v){
            graph[u].push_back(v);
            if(!flag){
                if(v == u || par.find(v) != par.end()){
                    cout << "INCORRECT SPANNING TREE! EXPECTED A DIRECTED ROOTED SPANNING TREE\n";
                    exit(0);
                }
                par[v] = u;
            }
        }     
    }
}


int main(){
	srand(time(0));
	ifstream ifile;
    ifile.open("inp-params.txt");           //input file
    ofile=fopen("out-log.txt","w+");		//log file

    string parse;
    getline(ifile, parse);
    stringstream stream;
    stream << parse;
    stream >> N >> Wr >> Ir >> Wb >> Ib >> lambdaBlue >> lambdaRed >> lambdaSnd >> p >> q;
    parseGraph(ifile, adjList);             //parse Graph
    parseGraph(ifile, spanning, false);     //parse spanning tree
    root = nodeID[0];                       //root

    vector <thread> T1, T2;
    vector <Node> process;
    for(int i=0; i<N; i++){
        process.push_back(Node(nodeID[i]));
    }
    for(int i=0; i<N; i++){
        T2.emplace_back(&Node::recieve, &process[i]);        //start receive threads. The server part of a node.
    }
    sleep(1);
    for(int i=0; i<N; i++){
        T1.emplace_back(&Node::sendMsg, &process[i]);     //start sync request threads
    }
    for (auto& v : T1){
        v.join();
    }
    for(int i=0; i<N; i++){
        process[i].setExitRecv();
    }
    for (auto& v : T2){
        v.join();
    }
    ll controlMsgCount=0,basicMsgCount=0;
    for(int i=0; i<N; i++){
        controlMsgCount += process[i].controlMsgCount;
        basicMsgCount += process[i].basicMsgCount;
    }
    fclose(ofile);
    ifile.close();
    cout << "Number of Control Messages: " << controlMsgCount << "\n";
    // cout << "Number of Basic Messages: " << basicMsgCount << "\n";
    cout << "Detection Time in nanoseconds: " << duration_cast<chrono::nanoseconds>(endTime - startTime).count() << "\n";
    return 0;
}