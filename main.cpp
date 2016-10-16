//
//  g++ -std=c++11 -w -o test main.cpp -pthread
//

#include <thread>
#include <mutex>
#include <condition_variable>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include <queue>
#include "ConnectionPooling.h"

using namespace std;

std::mutex mx;                        // mutex
std::condition_variable cond;         // conditional variable
std::queue< std::string > work_queue; // global work queue


class fakeSocket
{
	string _host;

public:

	fakeSocket( string host ) : _host( host ) 
	{  
		printf("%s : 0x%08x : constructed\n", _host.c_str(), this );
	};

	~fakeSocket()
	{
		printf("%s : 0x%08x : destructor\n", _host.c_str(), this );
	}

	bool connect()
	{
		printf("%s : 0x%08x : connected\n", _host.c_str(), this );
	}

	bool close()
	{
		printf("%s : 0x%08x : closing\n", _host.c_str(), this );
	}

	string host()
	{
		return _host;
	}
};



// "Threadable" base class
class threadable_base_class
{

public:
	threadable_base_class() : _thd(0) 
	{ _thd = new std::thread(threadable_base_class::work, this); }

	void run() { detach(); }

	void detach() { if(_thd) _thd->detach(); }
	void join()   { if(_thd) _thd->join();   }

	static void sleep( int ms)
	{
		std::this_thread::sleep_for( std::chrono::milliseconds( ms ) );
	}

	~threadable_base_class() { delete _thd; }

private:
	static void work( threadable_base_class* tp )
	{ tp->task(); }
	
	std::thread *_thd;

protected:
	virtual void task()=0;
};



ConnectionPool< fakeSocket, std::mutex, threadable_base_class > g_ConnPool;




// Worker class
class Worker : public threadable_base_class
{
	std::string  _name;
	
	// work ( thread ) of Worker
	void task()
	{
		while(true)
		{
			// Lock the mutex
			std::unique_lock<std::mutex> lock(mx);
			
			// Wait for work to do
			cond.wait( lock, []{ return !work_queue.empty(); } );
			
			// Get work task item
			std::string todo = work_queue.front();
			work_queue.pop();
			
			lock.unlock();
			
			// do the work
		//	std::cout << _name << " is going to " << todo << " right now" << std::endl << std::flush;

			fakeSocket* pSock = g_ConnPool.popConnection();			

			printf("%s using %s : 0x%08x\n", _name.c_str(), pSock->host().c_str(), pSock );


			// Take a break
			std::this_thread::sleep_for( std::chrono::milliseconds(2000) );
				
			g_ConnPool.pushConnection( pSock );
			printf("%s done with %s : 0x%08x\n", _name.c_str(), pSock->host().c_str(), pSock );
		}
	}
	
	Worker( );
	
	public:
	
	// Public constructor of Worker
	Worker( std::string name ) : threadable_base_class(), _name( name )  {}
};


int main( int argc, char **argv )
{
	// Create Workers
	std::vector<std::string> vec_workers_names{ "Sam", "Tom", "Jackie", "Lou", "Bruce" };
	
	// Establish work to be done
	std::vector<std::string> vec_work_tasks{ 
			"cook food", 
			"dig a hole", 
			"lay sod", 
			"clean the toilets", 
			"eat dinner", 
			"rake the yard", 
			"write cool code", 
			"take a shower", 
			"paint the fence", 
			"wax-a-on, wax-a-off", 
			"define foo", 
			"learn C++" };
	
	// Hire Workers
	for( auto worker_name : vec_workers_names )
	{
		Worker *pWrkr = new Worker( worker_name );
		pWrkr->detach();
	}

	// Deligate Work
	while(true)
	{
		for( auto work_task : vec_work_tasks )
		{
			std::this_thread::sleep_for( std::chrono::milliseconds(500) );
			std::lock_guard<std::mutex> lock(mx);
			work_queue.push(work_task);
			cond.notify_one();
		}
	}
}

