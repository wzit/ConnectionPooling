/*
Filename: ConnectionPooling.h
Created:  October 15th, 2016
Description: A generic collection of managed 
             network server connection containers
*/

#ifndef CONNECTIONPOOLING_H
#define CONNECTIONPOOLING_H

#include <queue>
#include <map>
#include <vector>
#include <set>



template < mutexType >
class Lock
{
	mutexType _mx;
	public:
	void lock(){ _mx.lock(); }
	void unlock(){ _mx.unlock(); }
};

template
class NoLock
{
	public:
	void lock(){}
	void unlock(){}
};


template < mutexType >
class ScopeLevelLocking
{
	mutexType &_rMutex;
	bool _bDetached;
	public:

	ScopeLevelLocking( mutexType & mutex ) : _rMutex(mutex), _bDetached(false)
	{
		_rMutex.lock();
	}

	~ScopeLevelLocking()
	{
		if( !_bDetached )
			_rMutex.unlock();
	}

	mutexType & detachMutex( bool bSet = true )
	{
		_bDetached = bSet;
		return _rMutex;
	}
};

template < mutexType >
class NoLocking
{
	mutexType &_rMutex;

	public:

	NoLocking( mutexType & mutex ) {}

	~NoLocking() {}
	
	mutexType & detachMutex( bool bSet ) {}
};



// This template makes a class method threadable
template < class Type >
class ThisThreader : public WorkerThread
{
    Type  *_obj;
    typedef void(Type::* Method)();
    Method _method;

    void task()
    {
        ((*_obj).*_method)();
    }

    public:

    ThisRunner(Type *obj, Method method )
        : _obj( obj ), _method( method )
    { }

    void run( bool bDetach = true )
    {
        this->start( bDetach );
    }
	
	static void sleep( int ms )
	{
		WorkerThread::sleep();
	}
};




template <class connType, class Lock, class LockPolicy > 
class ConnectionQueue
{
    std::queue< connType* > _Q;
    Lock _mx;
    bool _enable_state;

    public:

    ConnectionQueue( bool enabled = false )
        : _enable_state( enabled )  {}

    void pushConnection( connType* pConn )
    {
        LockPolicy  scopelock(_mx);
        _Q.push( pConn );
    }

    connType* popConnection()
    {
        connType *pConn = NULL;
        LockPolicy  scopelock(_mx);

        if( _Q.empty() )
            return NULL;

        pConn = _Q.front();
        _Q.pop();

        return pConn;
    }

    bool empty()
    {
        LockPolicy  scopelock(_mx);
        return _Q.empty();
    }

    bool enabled()
    {
        return _enable_state;
    }

    bool disabled()
    {
        return !_enable_state;
    }

    void enable()  { _enable_state = true; }
    void disable() { _enable_state = false; }

};



template <class connType, class Lock, class LockPolicy > 
class ConnectionRoundRobin
{
    Lock _mx;

    // Host name based lookup connection pool map typedef
    typedef std::map< std::string, ConnectionQueue< connType, NoLock, NoLocking > > MapRR;
    typedef MapRR::iterator ItRR;

    MapRR _mapRR;
    ItRR  _itRR;

public:

    ConnectionRoundRobin()
    {
        _itRR = _mapRR.begin();
    }

    ~ConnectionRoundRobin()
    {
    }

    connType* popConnection(const char* szHost = NULL)
    {
        connType* pConn = NULL;
        LockPolicy  scopelock( _mx );

        if( _mapRR.empty() ) // prevent a nasty loop when there are not Remotes
            return NULL;

        // Direct connection retrieval for closing conns in task thread : ?????
        if( szHost != NULL )
        {
            if( _mapRR.find(szHost) != _mapRR.end() )
            {
                if( _mapRR[szHost].size() > 0 )
                {
                        pConn = _mapRR[szHost].popConnection();
                }
            }
            return pConn;
        }

        // Round-robin here
        if( ++_itRR == _mapRR.end() )
            _itRR = _mapRRset.begin();

        // if the queue is empty go to the next available queue with connections
        if( !_itRR->second.empty() && _itRR->second.enabled() )
        {
            pConn = _itRR->popConnection();
        }

        if( pConn == NULL )
        {
            // go to next queue in a round until we get a connection or come back to this one
            for( typename MapRR::iterator mapIt = _mapRR.begin();
                    mapIt != _mapRR.end(); mapIt++ )
            {
                // Look for a queue with connections
                if( !mapIt->second.empty() )
                {
                    // if the queue is not disabled pop a connection
                    if( mapIt->second.enabled() )
                    {
                        pConn = mapIt->second.popConnection();
                        return pConn;
                    }
                }
            }
        }

        return pConn;
    }

    // uses host value to place in proper map
    void pushConnection(connType* pConn)
    {
        LockPolicy  scopelock(_mx);

        _mapRR[pConn->host()].push( pConn );
    }

    void disableQueue( std::string host)
    {
        LockPolicy  scopelock(_mx);

        typename MapRR::iterator mapIt = _mapRR.find( host );

        if( mapIt != _mapRR.end() )
        {
            mapIt.disable();
        }
    }

    void enableQueue( std::string host)
    {
        LockPolicy  scopelock(_mx);

        typename MapRR::iterator mapIt = _mapRR.find( host );

        if( mapIt != _mapRR.end() )
        {
            mapIt.enable();
        }
    }

    inline bool empty()
    {
        LockPolicy  scopelock(_mx);

        for( typename MapRR::iterator it = _mapRR.begin(); it != _mapRR.end(); it++ )
        {
            if( !it.empty() )
                return false;
        }

        return true;
    }


};



template <class connType, class Lock, class LockPolicy > 
class ConnectionPool
{
	Lock _mx;
	
	ConnectionRoundRobin< connType, NoLock, NoLocking > _connections;
	
	public:
	
	ConnectionPool() {}
	~ConnectionPool() {}
	
	void setConnectionsPerQueue( int connsPerQ )
	{
		_connections.connections_per_queue( connsPerQ );
	}
	
	void start() 
	{ 
		ThreadRunner< ConnectionPool > threader( this, manager ); 
		threader.run();
	} 
	
	private:
	
	void manager()
	{
		while(true)
		{
			// Scope Level Locking
			{
				LockPolicy scope( _mx );
				_connections.maintain_connections();
			}
			
			ThreadRunner::sleep( 100 );
		}
	}
	
};


#endif  // CONNECTIONPOOLING_H


