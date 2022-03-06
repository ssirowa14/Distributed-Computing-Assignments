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
    ll from, color;
};

class Node{             //class for process
	ll port, id, parent;          //port for server to receive requests
    atomic <ll> C, D, dparent, state, childLeft;   //state 0:White, 1:red, 2:blue
                                                    //childLeft is number of children remaining to send a terminate
    atomic <bool> exitSend, exitRecv;               //flags to exit send and recv
    mutex mtx;                                      //mutex to protect critical section

public:
    atomic <ll> controlMsgCount, basicMsgCount;

    Node(int i){        //constructor
        state = 0;
        id = i;
        C = 0;
        D = 0;
        basicMsgCount = controlMsgCount = 0;
        exitSend = false;
        exitRecv = false;
        dparent = i;
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
        id = copy.id;
        C = 0;
        D = 0;
        basicMsgCount = controlMsgCount = 0;
        exitSend = false;
        exitRecv = false;
        dparent = id;
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

    //function to send messages to other process
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
            printf("Error : sendRequest : connect : %lld,%lld,%lld : %s %lld\n", (ll)id, (ll)serverPort-BASE, color, strerror(errno), (ll)errno);
            close(sock);
            return -1; 
        }
        Packet buff;
        buff.from = from;       //sender process
        buff.color = color;     //color of process or type of msg
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
        shuffle(temp.begin(), temp.end(), default_random_engine(time(0)));      //shuffle process ids
        ll sleepForTime = ceil(ran_exp(Wr)+1);                                  //sleep before red injection
        this_thread::sleep_for(chrono::milliseconds(sleepForTime));
        int itr = 0;
        while(itr < Ir){                                                        //red injection
            D++;
            while(sendRequest(id, 1, temp[itr]+BASE) < 0);
            itr++;
        }
        sleepForTime = ceil(ran_exp(Wb)+1);
        this_thread::sleep_for(chrono::milliseconds(sleepForTime));         //sleep before blue injection
        startTime = system_clock::now();                                    //startTime of algorithm
        while(itr < Ir+Ib-1){                                               //blue injection
            while(sendRequest(id, 2, temp[itr]+BASE) < 0);
            itr++;
        }
        state = 2;                                                          //colour is blue. Root is blue 
    }

    //function to simulate send functionality of a process(sender thread)
    void sendMsg(){
        if(id == root){         //initialization
            initialize();
        }
        while(state == 0);      //while white
        time_t my_time;
        auto timeinfo = localtime(&my_time);
        while(!exitSend){
            int color = state;
            shuffle(adjList[id].begin(), adjList[id].end(), default_random_engine(time(0)));
            int limit=ceil((color==1 ? p : q)*(double)adjList[id].size());      //take first q% from shuffle adjcency list
            limit = min((int)adjList[id].size(), limit);
            for(int i=0; i<limit; i++){                                         //send coloured msgs
                if(color == 1 && adjList[id][i] == root){                       //don't send red msg to root
                    continue;
                }
                if(color == 1){                                                 //red msg
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld sends Red msg to Cell %d at time %d:%d:%d\n",id,adjList[id][i],timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld sends Red msg to Cell %d at time %d:%d:%d\n",id,adjList[id][i],timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    D++;                        //treated as a basic msg. D incremented
                    basicMsgCount++;
                }
                else{                               //blue msg
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld sends Blue msg to Cell %d at time %d:%d:%d\n",id,adjList[id][i],timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld sends Blue msg to Cell %d at time %d:%d:%d\n",id,adjList[id][i],timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                }
                while(sendRequest(id, color, adjList[id][i]+BASE) < 0);     //send colour msg
                if(color == 1){                                             //sleep after sending coloured msg
                    ll sleepForTime = ceil(ran_exp(lambdaRed)+1);
                    this_thread::sleep_for(chrono::milliseconds(sleepForTime));
                }
                if(color == 2){
                    ll sleepForTime = ceil(ran_exp(lambdaBlue)+1);
                    this_thread::sleep_for(chrono::milliseconds(sleepForTime));
                }
            }
            mtx.lock();
            if(C == 1 && D == 0 && state == 2){                 //send ack and node concludes that it has completed computation. Wakes up on receipt of next red msg 
                C = 0;
                int dparentOld = dparent;
                dparent = id;
                mtx.unlock();
                while(sendRequest(id, 5, dparentOld+BASE) < 0);
            }
            else{
                mtx.unlock();
            }
            mtx.lock();
            if(C == 0 && D == 0 && state == 2 && childLeft == 0 && parent != -1){           //send terminate msg to static parent
                mtx.unlock();
                controlMsgCount++;
                while(sendRequest(id, 6, parent+BASE) < 0);
                parent = -1;
            }
            else{
                mtx.unlock();
            }            
        }
        for(int i=0; i<spanning[id].size(); i++){                                       //send shut down msg to children
            while(sendRequest(id, 7, spanning[id][i]+BASE) < 0);
        }
        time (&my_time);
        timeinfo = localtime (&my_time);
        fprintf(ofile, "Cell %lld initiates termination at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
        printf("Cell %lld initiates termination at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
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
            if(buff.color == 1){   //basic (Red Msg)
                time (&my_time);
                timeinfo = localtime (&my_time);
                fprintf(ofile, "Cell %lld Receives Red msg from Cell %lld at time %d:%d:%d\n",id,buff.from,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                printf("Cell %lld Receives Red msg from Cell %lld C = %lld, D = %lld, childLeft = %lld, state = %lld, parent = %lld, dparent = %lld\n",id,buff.from,(ll)C,(ll)D, (ll)childLeft, (ll)state, (ll)parent, (ll)dparent);
                mtx.lock();
                if(C == 0){         //first msg since it bacame passive or white. dparent is set and C is set to 1
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld turns Red at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld turns Red at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    state = 1;
                    C = 1;
                    dparent = buff.from;
                    mtx.unlock();
                }
                else if(state != 1){            //send Acks
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld turns Red at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld turns Red at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    state = 1;
                    mtx.unlock();
                    while(sendRequest(id, 5, buff.from+BASE) < 0);
                }
                else{           //send Acks
                    mtx.unlock();
                    while(sendRequest(id, 5, buff.from+BASE) < 0);
                }
            }
            else if(buff.color == 2){   //become passive (Blue msg)
                time (&my_time); 
                timeinfo = localtime (&my_time);
                fprintf(ofile, "Cell %lld Receives Blue msg from Cell %lld at time %d:%d:%d\n",id,buff.from,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                printf("Cell %lld Receives Blue msg from Cell %lld C = %lld, D = %lld, childLeft = %lld, state = %lld, parent = %lld, dparent = %lld\n",id,buff.from,(ll)C,(ll)D, (ll)childLeft, (ll)state, (ll)parent, (ll)dparent);
                if(state != 2){
                    time (&my_time);
                    timeinfo = localtime (&my_time);
                    fprintf(ofile, "Cell %lld turns Blue at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    printf("Cell %lld turns Blue at time %d:%d:%d\n",id,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                    state = 2;
                }
            }
            else if(buff.color == 5){   //ack
                controlMsgCount++;
                D--;                    //D in decremented on receipt of Ack
                printf("Cell %lld Receives Ack msg from Cell %lld C = %lld, D = %lld, childLeft = %lld, state = %lld, parent = %lld, dparent = %lld\n",id,buff.from,(ll)C,(ll)D, (ll)childLeft, (ll)state, (ll)parent, (ll)dparent);
            }
            else if(buff.color == 6){   //terminate
                childLeft--;
                if(childLeft == 0 && id == root){           //root concludes termination
                    endTime = system_clock::now();          //end time of algorithm
                    exitSend = true;                        //exit sender thread
                }
                printf("Cell %lld Receives Terminate msg from Cell %lld C = %lld, D = %lld, childLeft = %lld, state = %lld, parent = %lld, dparent = %lld\n",id,buff.from,(ll)C,(ll)D, (ll)childLeft, (ll)state, (ll)parent, (ll)dparent);
            }
            else if(buff.color == 7){   //shut down
                exitSend = true;
                printf("Cell %lld Receives Shut Down msg from Cell %lld C = %lld, D = %lld, childLeft = %lld, state = %lld, parent = %lld, dparent = %lld\n",id,buff.from,(ll)C,(ll)D, (ll)childLeft, (ll)state, (ll)parent, (ll)dparent);
            }
            else{
                cout << "ERROR msg recvd by" << id << "\n";
            }
            close(sockid);          //close socket
        }
        printf("--out-%lld\n", id);
        close(server_id);           //close listening socket
    }
    void setExitRecv(){
        exitRecv = true;
    }
};

void parseGraph(ifstream &ifile, unordered_map <int, vector<int>> &graph, bool flag=true){          //function to parse graph
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
                if(v == u || par.find(v) != par.end()){         //rooted spanning tree expected
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
    parseGraph(ifile, adjList);         //parse graph
    parseGraph(ifile, spanning, false);     //parse spanning tree
    root = nodeID[0];           //root 

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