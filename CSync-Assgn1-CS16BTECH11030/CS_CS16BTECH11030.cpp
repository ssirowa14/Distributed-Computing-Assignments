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
typedef unsigned long long ull;
typedef long long ll;
#define BASE 11000      //port are assigned starting from 11000
using namespace std;
using namespace chrono;

int N, K, Lp, Lq, Lsnd, Ldrift, Lwkdrift;
FILE *ofile;

struct Packet{          //Packet sent over network
    ull T2, T3, from, reqID;
};

class Node{             //class for process
	ull port;          //port for server to receive requests
    atomic <ll> driftFactor, errorFactor;
    atomic <bool> flag;     //denotes when to exit infinite loops

public:
    vector <ull> logTime;

    Node(int i){        //constructor
        flag = true;
        driftFactor = 0;
        errorFactor = 1;
        port = BASE+i;
    }

    Node(const Node &copy){     //copy constructor
        flag = true;
        driftFactor = 0;
        errorFactor = 1;
        port = copy.port;
    }

    double ran_exp(float lambda){       //function to return exponential distribution time
        default_random_engine gen;
        exponential_distribution <double> dist(1.0/lambda);
        return dist(gen);
    }

    ull read(){                 //function to read clock at a process. An error factor is added to make simulation more realistic
        system_clock::time_point tp = system_clock::now();
        system_clock::duration dtn = tp.time_since_epoch();

        return (ll)dtn.count()+driftFactor+errorFactor;
    }

    void update(ll delta){          //function to update error factor
        errorFactor += delta;
    }

    int incrementDriftFactor(){     //function to update drift factor
        while(flag){
            double clockDrift = ran_exp(Ldrift);
            if(rand()%2){
                driftFactor += ceil(clockDrift);
            }
            else{
                driftFactor -= ceil(clockDrift);
            }
            ll sleepForTime = ceil(ran_exp(Lwkdrift)+10);
            this_thread::sleep_for(chrono::milliseconds(sleepForTime));
        }
    }

    //function to send synchronization request
    int sendRequest(ull from, ull reqID, ull serverPort, ull &T1, ull &T2, ull &T3, ull &T4){
        //creating socket
        int sock;
        struct sockaddr_in serv_addr;
        if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
            printf("Error : sendRequest : socket : %lld,%lld : %s\n", (ll)port-BASE, (ll)serverPort-BASE, strerror(errno));
            return -1; 
        }
        serv_addr.sin_family = AF_INET; 
        serv_addr.sin_port = htons(serverPort); 
        if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0){
            printf("Error : sendRequest : inet_pton : %lld,%lld : %s\n", (ll)port-BASE, (ll)serverPort-BASE, strerror(errno));
            close(sock);
            return -1; 
        }
        if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
            printf("Error : sendRequest : connect : %lld,%lld : %s\n", (ll)port-BASE, (ll)serverPort-BASE, strerror(errno));
            close(sock);
            return -1; 
        }
        Packet buff;
        buff.from = from;
        buff.reqID = reqID; 
        T1 = read();            //update T1
        ll sleepForTime = ceil(ran_exp(Lsnd)+1);
        this_thread::sleep_for(chrono::microseconds(sleepForTime));
        while(send(sock, &buff, sizeof(buff), 0) < 0);      //send request
        while(recv(sock,&buff,sizeof(buff),0) < 0);         //receive request
        T4 = read();                //update T4
        T2 = buff.T2;
        T3 = buff.T3;
        close(sock);
        return 1;
    }

    //function to synchronize distributed clocks
    void synchronizeRequest(ull pid){
        ull T1, T2, T3, T4;
        ll delta;
        time_t my_time;
        auto timeinfo = localtime(&my_time);
        for(int i=0; i<K; i++){
            delta = 0;
            for(int j=0; j<N; j++){
                if(j == pid){
                    continue;
                }
                time (&my_time);
                timeinfo = localtime (&my_time);
                fprintf(ofile, "Server %llu Requests %d clock synchronization from Server %d at time %d:%d:%d\n",pid,i,j,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                while(sendRequest(pid, i, BASE+j, T1, T2, T3, T4) < 0);
                time (&my_time);
                timeinfo = localtime (&my_time);
                fprintf(ofile, "Server %llu Receives %d clock synchronization from Server %d at time %d:%d:%d\n",pid,i,j,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
                delta += ((ll)T2-(ll)T4-(ll)T1+(ll)T3)/2;
                printf("Success : pid = %lld : Round = %d : Node = %d\n", pid, i, j);
                //sleep
                ll sleepForTime = ceil(ran_exp(Lp)+1);
                this_thread::sleep_for(chrono::milliseconds(sleepForTime));
            }
            update(delta/(N-1));    //update delta
            logTime.push_back(read());      //read clock value after sync round
        }
        // printf("Request : Exit : %lld\n", (ll)port-BASE);
    }

    //function to receive request. It implements server
    int recieve(ull pid){
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
        while(flag){
            len=sizeof(clientAddr);
            sockid = accept(server_id,(struct sockaddr*)&clientAddr, (socklen_t*)&len);
            //Connection Established
            if(sockid < 0){
                continue;
            }
            //get request
            while(recv(sockid, &buff, sizeof(buff), 0) < 0);
            buff.T2 = read();   //update T2
            time (&my_time);
            timeinfo = localtime (&my_time);
            fprintf(ofile, "Server %llu Receives %llu clock synchronization request from Server %llu at time %d:%d:%d\n",pid,buff.reqID,buff.from,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
            ll sleepForTime = ceil(ran_exp(Lq)+1);
            this_thread::sleep_for(chrono::milliseconds(sleepForTime)); //sleep
            time (&my_time);
            timeinfo = localtime (&my_time);
            fprintf(ofile, "Server %llu Replies %llu clock synchronization request to Server %llu at time %d:%d:%d\n",pid,buff.reqID,buff.from,timeinfo->tm_hour, timeinfo->tm_min,timeinfo->tm_sec);
            buff.T3 = read();
            sleepForTime = ceil(ran_exp(Lsnd)+1);
            this_thread::sleep_for(chrono::microseconds(sleepForTime));
            while(send(sockid, &buff, sizeof(buff),0) < 0);     //send sync response
            close(sockid);          //close socket
        }
        close(server_id);           //close listening socket
        // printf("Receive : Exit : %lld\n", (ll)port-BASE);
    }
    void setFlag(){                 //function to setflag to false
        flag = false;
    }
};


int main(){
	srand(time(0));
	ifstream ifile;
    ifile.open("inp-params.txt");           //input file
    ofile=fopen("out-log.txt","w+");		//log file
    ifile >> N >> K >> Lp >> Lq >> Lsnd >> Ldrift;
    vector <thread> T1, T2, T3;
    vector <Node> process;
    for(int i=0; i<N; i++){
        process.push_back(Node(i));
    }
    for(int i=0; i<N; i++){
        T2.emplace_back(&Node::recieve, &process[i], i);        //start receive threads. The server part of a node.
    }
    sleep(1);
    for(int i=0; i<N; i++){
        T1.emplace_back(&Node::synchronizeRequest, &process[i], i);     //start sync request threads
        T3.emplace_back(&Node::incrementDriftFactor, &process[i]);      //start threads to simulate drift
    }
    for (auto& v : T1){
        v.join();
    }
    for(int i=0; i<N; i++){
        process[i].setFlag();
    }
    for (auto& v : T3){
        v.join();
    }
    for (auto& v : T2){
        v.join();
    }

    //Calculating Variance and other stats
    fprintf(ofile, "\n\n\nstats of Time at Each Server after every round. Each row contains tab separated entries. Entry(i,j) denotes time at server i after round j. i denotes row number and j denotes column number\n\n\n");

    for(int i=0; i<N; i++){
        for(int j=0; j<K; j++){
            fprintf(ofile, "%llu\t", process[i].logTime[j]);
        }
        fprintf(ofile, "\n");
    }
    fprintf(ofile, "\n\n\n(Mean, Variance)\n\n" );
    double finalMean, finalVar;
    for(int i=0; i<K; i++){
        ull sum = 0;
        double mean, var=0.0;
        for(int j=0; j<N; j++){
            sum += process[j].logTime[i];
        }
        mean = double(sum)/double(N);
        for(int j=0; j<N; j++){
            // var += ((process[j].logTime[i]-mean)*(process[j].logTime[i]-mean));
            var += abs(((double)process[j].logTime[i]-mean));
        }
        var /= double(N);
        fprintf(ofile, "Round %d (%lf , %lf)\n", i, mean, var);
        finalMean = mean;
        finalVar = var;
    }
    printf("Final Variance = %lf\n", finalVar);
    fclose(ofile);
    ifile.close();
    return 0;
}