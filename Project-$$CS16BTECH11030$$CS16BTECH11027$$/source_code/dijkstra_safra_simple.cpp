#include<bits/stdc++.h>
#include<thread>
#include<iostream>
#include <atomic>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <netinet/in.h> 
#include <sys/time.h>
#include <sys/resource.h>
#include <chrono>
#include <math.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h> 
#include <stdlib.h> 
#include <netinet/in.h>
#include <fcntl.h>
#include <mutex>
#include <fstream>

using namespace std;
typedef unsigned long long ull;
typedef long long ll;


int N , wait_to_r , wait_to_b , red_no , blue_no , lambda_blue , lambda_red , lambda_snd; // global variables
atomic<bool> term_detect;
atomic<ll> tot_msg;
FILE *f = fopen("out-log.txt", "w");




double ran_exp(float lambda)   															  // exponential time						
{
	default_random_engine generate;
	srand(5);
	exponential_distribution <double> distribution(lambda);
	return distribution(generate);
}


struct token                                                                              // token structure
{
	ll counterSum = 0;
	int color = 0;
};                                                                        

struct msg                                                                                // msg structure
{
	int msgType;																		  // 0 for normal 1 for token msg
	int sender_id;
	int sender_state;
	token tokenVal;
	
};

class Node{

public:

	int id;

	atomic<int>state;                                                                     // to simualte cell state (white , red , blue)
	atomic<ll> counter;                                                                   // send - recv                               
	atomic<int> color;																	  // color : white(0) or black(1) (dont confuse with state)
	vector<int> children;            
	atomic<bool> idle;
	atomic<bool> flag;															          // to stop recv
	atomic<bool> tokenHas;
	token tokenVal;
	int parent = -1;
	int port;
	mutex mtx;                                                                            // true if passive

	Node(int port1 , int id1){        													  // constructor
        id = id1;
        port = port1;
        counter = 0;
        color = 0;
        state = 0;
        idle = false;
        flag = true;
        if(id1 == 0){
        	tokenHas = true;
        }
        else{
        	tokenHas = false;
        }
    }

    Node(const Node &copy){     //copy constructor
        id = copy.id;
        port = copy.port;
        if(id == 0){
        	tokenHas = true;
        }
        else{
        	tokenHas = false;
        }
        tokenVal = copy.tokenVal;
        counter = 0;
        color = 0;
        state = 0;
        idle = false;
        flag = true;
    }

    int send_(msg m1, int recv, vector <Node> &nodeList){
    	int client_sock;                                                      
		struct sockaddr_in servAddr;

		if ((client_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){	
            printf("Error : sendRequest : socket : %lld : %s\n", (ll)port, strerror(errno));
            close(client_sock);
            return -1; 
    	}

		servAddr.sin_family = AF_INET;
		servAddr.sin_port = htons(nodeList[recv].port);                                   // select port 

		if(inet_pton(AF_INET, "127.0.0.1", &servAddr.sin_addr) <= 0)
		{
			printf("Error : sendRequest : inet_pton : %lld : %s\n", (ll)port, strerror(errno));
            close(client_sock);
            return -1;
        }

		while(connect(client_sock , (struct sockaddr *)&servAddr, sizeof(servAddr)) == -1){
			printf("Error : sendRequest : inet_pton : %lld : %s\n", (ll)port, strerror(errno));
			close(client_sock);
			return -1;
		}

		usleep(10000* ran_exp(lambda_snd));
		if(send(client_sock,(void*)&m1 , sizeof(m1) , 0) < 0){
			printf("Error : sendRequest : send : %lld : %s\n", (ll)port, strerror(errno));
			close(client_sock);
			return -1;
		}
		time_t  comTime = time(0);
		tm* ltm = localtime(&comTime);

		if(m1.sender_state == 1){	
			printf("Process %d sends RED message to Process %d at time %d:%d\n", id , recv , 1 + ltm->tm_min,1 + ltm->tm_sec);
			fprintf(f , "Process %d sends RED message to Process %d at time %d:%d\n", id , recv , 1 + ltm->tm_min,1 + ltm->tm_sec);
		}
		else{	
			printf("Process %d sends BLUE message to Process %d at time %d:%d\n", id , recv , 1 + ltm->tm_min,1 + ltm->tm_sec);
			fprintf(f , "Process %d sends BLUE message to Process %d at time %d:%d\n", id , recv , 1 + ltm->tm_min,1 + ltm->tm_sec);
		}

		close(client_sock);
		client_sock = -1;
		return 1;
    }

    int sendMsg(vector<Node> nodeList){

    	vector<int> idList;                                                            /* just creating a vector of ids out of which one will be selected to
    																					  send message to (after shuffling)*/ 
    	int i = 1;																	

    	while(i < N)
    	{
    		if(i != id)
    		{
    		   idList.push_back(i);
    		}

    		i++;

    	}

    	while(!term_detect)														     	 //loop
    	{
    		int localState = state;
    		if(localState == 0)                                                          //if white cell or state = 0. do nothing
    		{
    			continue;
    		}

    		if(localState == 1)                                                         //sleep
    		{
    			usleep(10000*ran_exp(lambda_red));
    			counter += 1;														// if state is 1 -> means red -> means active -> increment counter
    		}
    		else
    		{
    			
    			usleep(10000*ran_exp(lambda_blue));
    		}

    		random_shuffle(idList.begin(), idList.end());                         //shuffle idList to get a random ID to send message to
    		int recv = idList[0];
    		msg m1;                                                             
			m1.msgType = 0;
			m1.sender_id = id;
			m1.sender_state = localState;                                            // sender_id and state of sender in msg
			if(send_(m1, recv, nodeList) < 0){
				if(localState == 1)
					counter -= 1;
			}

			//dijkstra safra termination detection
			if(tokenHas == true){
				msg m1;
				m1.msgType = 1;
				m1.sender_id = id;
				mtx.lock();
				bool localIdle = idle;
				int localColor = color;
				ll localCounter = counter;
				mtx.unlock();
				if(id == N-1 && localIdle == true){				//coordinator
					tokenHas = false;
					if(localColor == 0 &&(tokenVal.counterSum+localCounter) == 0 && tokenVal.color == 0){
						term_detect = true;
						printf("Termination detected\n");
						fprintf(f , "Termination detected\n");
					}
					else{
						tot_msg += 1;
						tokenVal.counterSum = 0;
						tokenVal.color = 0;
						color = 0;
						m1.tokenVal = tokenVal;
						while(send_(m1, (id+1)%N, nodeList)<0);
					}
				}
				else if(localIdle == true){
					tokenHas = false;
					tokenVal.counterSum += localCounter;
					(localColor == 1)?tokenVal.color=1:1;
					color = 0;
					m1.tokenVal = tokenVal;
					tot_msg += 1;
					while(send_(m1, (id+1)%N, nodeList)<0);
				}

			}
    	}

    	return 1;
    }

    int recvMsg(){

    	//cout <<"Came here\n";
    	struct sockaddr_in servAddr;
    	int serv_sock , opt = 1;  
    	if((serv_sock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)) < 0){
            printf("Error : Receive : socket : %lld : %s\n", (ll)port, strerror(errno));
            return -1;
        }

        if (setsockopt(serv_sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)) < 0){
            printf("Error : Receive : setsockopt : %lld : %s\n", (ll)port, strerror(errno));
            close(serv_sock);
            return -1;
        } 

        servAddr.sin_family = AF_INET; 
		servAddr.sin_addr.s_addr = INADDR_ANY;
		servAddr.sin_port = htons(port); 

		if (bind(serv_sock, (struct sockaddr *)&servAddr, sizeof(servAddr)) < 0){
            printf("Error : Receive : bind : %lld : %s\n", (ll)port, strerror(errno));
            close(serv_sock);
            return -1; 
        }

        if(listen(serv_sock, 2*N) < 0){
            printf("Error : Receive : listen : %lld : %s\n", (ll)port, strerror(errno));
            close(serv_sock);
            return -1;
        }

        while(flag)
		{
			//usleep(100000*ran_exp(lambda2));
			int addrLen = sizeof(servAddr);
			int created_socket = accept(serv_sock, (struct sockaddr *)&servAddr, (socklen_t*)&addrLen); //accept incoming request

			if(created_socket == -1)                                                                //error_handling
			{
				if(errno == EWOULDBLOCK)
				{
					usleep(100);
				}
				else
				{
				     perror("error when accepting connection");
			         return -1;
				}
			}
			else
			{
	 			msg reply;
	    	    while(recv(created_socket, (void*)&reply, sizeof(reply), 0) < 0)                  //recv
	    	    {
	    	    	printf("error in recievning\n");
	    	    }
	    	    time_t  comTime = time(0);
	 			tm* ltm = localtime(&comTime);

				if(reply.msgType == 0){
					state = reply.sender_state;                                                     // msg recvd

					if(reply.sender_state == 1)                                                     //if the process who sent msg was red/active 
					{
						printf("Process %d recvs RED msg from Process %d at time %d:%d\n", (int)id, (int)reply.sender_id, 1 + ltm->tm_min, 1 + ltm->tm_sec);
						fprintf(f , "Process %d recvs RED msg from Process %d at time %d:%d\n", (int)id, (int)reply.sender_id, 1 + ltm->tm_min, 1 + ltm->tm_sec);
						mtx.lock();
						idle = false;                                                               //now this process can become active
						color = 1;                                                                  // since we recv msg, color the recving process black
						counter -= 1;
						mtx.unlock();			                                                    //decrement counter
					}
					else                                                                            //else the msg was recvd from passive process (makes no sense)
					{		
						printf("Process %d recvs BLUE msg from Process %d at time %d:%d\n", (int)id, (int)reply.sender_id, 1 + ltm->tm_min, 1 + ltm->tm_sec);
						fprintf(f , "Process %d recvs BLUE msg from Process %d at time %d:%d\n", (int)id, (int)reply.sender_id, 1 + ltm->tm_min, 1 + ltm->tm_sec);
						idle = true;                                                                //process becomes passive
					}
				}
				else{																				//this is a token
					printf("Process %d recvs TOKEN msg from Process %d at time %d:%d\n", (int)id, (int)reply.sender_id, 1 + ltm->tm_min, 1 + ltm->tm_sec);
					fprintf(f , "Process %d recvs TOKEN msg from Process %d at time %d:%d\n", (int)id, (int)reply.sender_id, 1 + ltm->tm_min, 1 + ltm->tm_sec);
					tokenVal = reply.tokenVal;
					tokenHas = true;
				}

				close(created_socket);
				created_socket = -1;
			}
		}

		close(serv_sock);                                                                            //close
		serv_sock = -1;
		return 1;
    }

};

vector<Node> nodeList;

void init_color(vector<Node> &nodeList){                                    //just intiating some process as active or passive

	vector<int> idList;

	for(int i = 1; i < N ; i++)
	{
		idList.push_back(i);
	}

	random_shuffle(idList.begin(), idList.end()); 
	int lambda_wait = 1/wait_to_r;
	usleep(10000*ran_exp(lambda_wait));
	cout << N << " " << red_no << " " << blue_no << "\n";
	int n = (red_no * N) / 100.0;
	// cout << n << "\n";
	for(int i = 0; i < n; i++)												//red
	{
		nodeList[idList[i]].state = 1; 
		nodeList[idList[i]].idle = false;
	}

	idList = vector <int> (idList.begin()+n, idList.end());
	random_shuffle(idList.begin(), idList.end()); 
	lambda_wait = 1/wait_to_b;
	usleep(10000*ran_exp(lambda_wait));
	n = (blue_no * N)/ 100.0;
	// cout << n << "\n";
	// exit(0);
	srand(5);

	nodeList[0].state = 2;
	nodeList[0].idle = true;
	for(int i = 0; i < n ; i++)												//blue
	{
		nodeList[idList[i]].state = 2; 
		nodeList[idList[i]].idle = true;
	}
}





int main(){

	term_detect = false;
	ifstream inFile;                                                                           
	inFile.open("inp-params.txt");
	inFile >> N >> wait_to_r >> red_no >> wait_to_b >> blue_no >> lambda_red >> lambda_blue >> lambda_snd; 
	int start_port = 7800; 
	// cout << N << " " << red_no << " " << blue_no << "\n";
	tot_msg = 0;

	for(int i = 0; i < N ; i++)
	{
		Node nd(start_port++ , i);                                                       //object creation
		nodeList.push_back(nd);
	}

	init_color(nodeList);
	// thread t1(djik_safra);
	vector<thread> sendArr(N);
	vector<thread> recvArr(N);

	auto start = std::chrono::system_clock::now();

	for(int i = 0 ; i < N; i++)
	{
		recvArr[i] = thread(&Node::recvMsg , &nodeList[i]);                    //creation of threads

	}

	for(int i = 0 ; i < N; i++)
	{
		sendArr[i] = thread(&Node::sendMsg , &nodeList[i] , nodeList);         //creation of threads
	}

	// t1.join();

	for(int i = 0; i < N; i++)
	{
		sendArr[i].join();
	}

	for(int i = 0; i < N; i++)
	{
		nodeList[i].flag = false;
	}
	
	for(int i = 0; i < N ; i++)
	{
		recvArr[i].join();
	}

	auto exit = std::chrono::system_clock::now();
	printf("-------------------TERMINATED----------------------------\n");
	cout << "Time required in ms: " << chrono::duration_cast<std::chrono::milliseconds>(exit - start).count() << endl;
	cout <<"Total Ctrl Messages "<< tot_msg << endl;
	inFile.close();
	fclose(f);
	return 0;
}
