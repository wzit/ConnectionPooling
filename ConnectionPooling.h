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



template < class mutexType >
class Lock
{
	mutexType _mx;
	public:
	void lock(){ _mx.lock(); }
	void unlock(){ _mx.unlock(); }
};

template < class whatever >
class NoLock
{
	public:
	void lock(){}
	void unlock(){}
};


template < class mutexType >
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

template < class mutexType >
class NoLocking
{
	public:

	NoLocking( mutexType mutex ) {}

	~NoLocking() {}
	
	mutexType & detachMutex( bool bSet ) {}
};



// This template makes a class method threadable
template < class Type, class TaskThreadRunner >
class ThisThreader : public TaskThreadRunner
{
    Type  *_obj;
    typedef void(Type::* Method)();
    Method _method;

    void task()
    {
        ((*_obj).*_method)();
    }

    public:

    ThisThreader(Type *obj, Method method )
        : _obj( obj ), _method( method )
    { }

    void run( bool bDetach = true )
    {
        this->start( bDetach );
    }
	
	static void sleep( int ms )
	{
		TaskThreadRunner::sleep();
	}
};




template <class connType, class Locker, class LockPolicy > 
class ConnectionQueue
{
    std::queue< connType* > _Q;
    Locker _mx;
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

    bool size()
    {
        LockPolicy  scopelock(_mx);
        return _Q.size();
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



template < class connType, class Locker, class LockPolicy, class TaskThreadRunnner > 
class ConnectionLoadBalancer
{
    Locker _mx;

    typedef ConnectionQueue< connType, NoLock< bool >, NoLocking< NoLock< bool > > > ConnQue;

    // Host name based lookup connection pool map typedef
    typedef std::map< std::string, ConnQue > MapLB;
    typedef typename MapLB::iterator ItLB;

    MapLB _mapLB;
    ItLB  _itLB;

public:

    ConnectionLoadBalancer()
    {
        _itLB = _mapLB.begin();
    }

    ~ConnectionLoadBalancer()
    {
    }

    connType* popConnection(const char* szHost = NULL)
    {
        connType* pConn = NULL;
        LockPolicy  scopelock( _mx );

        if( _mapLB.empty() ) // prevent a nasty loop when there are not Remotes
            return NULL;

        // Direct connection retrieval for closing conns in task thread : ?????
        if( szHost != NULL )
        {
            if( _mapLB.find(szHost) != _mapLB.end() )
            {
                if( _mapLB[szHost].size() > 0 )
                {
                        pConn = _mapLB[szHost].popConnection();
                }
            }
            return pConn;
        }

        // Round-robin here
        if( ++_itLB == _mapLB.end() )
            _itLB = _mapLB.begin();

        // if the queue is empty go to the next available queue with connections
        if( ( !_itLB->second.empty() ) && _itLB->second.enabled() )
        {
            pConn = _itLB->second.popConnection();
        }

        if( pConn == NULL )
        {
            // go to next queue in a round until we get a connection or come back to this one
            for( typename MapLB::iterator mapIt = _mapLB.begin();
                    mapIt != _mapLB.end(); mapIt++ )
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
    bool pushConnection(connType* pConn)
    {
        LockPolicy  scopelock(_mx);

        typename MapLB::iterator mapIt = _mapLB.find( pConn->host() );

        if( mapIt != _mapLB.end() )
        {
            mapIt->second.pushConnection( pConn );
			return true;
        }
		else
		{
			return false;
		}
    }

    bool disableQueue( std::string host)
    {
        LockPolicy  scopelock(_mx);

        typename MapLB::iterator mapIt = _mapLB.find( host );

        if( mapIt != _mapLB.end() )
        {
            mapIt->second.disable();
			return true;
        }
		else
		{
			return false;
		}
    }

    bool enableQueue( std::string host)
    {
        LockPolicy  scopelock(_mx);

        typename MapLB::iterator mapIt = _mapLB.find( host );

        if( mapIt != _mapLB.end() )
        {
            mapIt->second.enable();
			return true;
        }
		else
		{
			return false;
		}
    }

    inline bool empty()
    {
        LockPolicy  scopelock(_mx);

        for( typename MapLB::iterator it = _mapLB.begin(); it != _mapLB.end(); it++ )
        {
            if( !it->second.empty() )
                return false;
        }

        return true;
    }

    void addQueue( std::string hostQueue )
    {
		LockPolicy  scopelock(_mx);
		_mapLB[ hostQueue ]; // Add an empty queue to the map
    }

    bool deleteQueue( std::string hostQueue )
    {
		LockPolicy  scopelock(_mx);
		
		ItLB it = _mapLB.find( hostQueue );
		
		if( it != _mapLB.end() )
		{
			ConnQue &connQue = it->second;
			
			while( !connQue.empty() )
			{
				connType *pConn = connQue.popConnection();
				
				if( pConn != NULL )
				{
					pConn->close();
					delete pConn;
				}
			}
			
			_mapLB.erase( it );

			return true;
		}

		return false;
    }

    void start() 
    { 
        ThisThreader< ConnectionLoadBalancer, TaskThreadRunnner > threader( this, manager ); 
        threader.run();
    } 

 private:

    void manager()
    {
        while(true)
        {   
 
            connType* pConn= NULL;

            // Scope Level Locking
            {
                LockPolicy scope( _mx );
                pConn = popConnection();
            }

            if( pConn != NULL )
            {
				bool alive = keepConnAlive( pConn );
				
				{   // Scope Locked
					LockPolicy scope( _mx );

					if( alive )
						pushConnection( pConn );
					else
						pushRecycle( pConn );
				}
            }

            ThisThreader< ConnectionLoadBalancer, TaskThreadRunnner >::sleep( 1000 );
        }
    }
};



template < class connType, class mutexType, class TaskThreadRunnner > 
class ConnectionPool
{
	Lock< mutexType > _mx;
	
	typedef ConnectionLoadBalancer< connType, Lock< mutexType >, ScopeLevelLocking< Lock< mutexType > >, TaskThreadRunnner > ConnLB;

	ConnLB _favoredConns;
	ConnLB _otherConns;
	
	public:
	
	ConnectionPool() { }

	~ConnectionPool() { }
		
	connType* popConnection()
	{
		connType *conn = NULL;

		if( ( conn = _favoredConns.popConnection() ) == NULL )
			conn = _otherConns.popConnection();

		return conn;
	}
	
	void pushConnection( connType* conn )
	{
		if( !_favoredConns.pushConnection(conn) )
		{
			if( !_otherConns.pushConnection(conn) )
			{
				conn->close();
				delete conn;
			}
		}
	}
	
	void disableQueue( std::string hostQueue )
	{
		if( !_favoredConns.disableQueue( hostQueue ) )
			_otherConns.disableQueue( hostQueue );
	}
	
	void enableQueue( std::string hostQueue )
	{
		if( !_favoredConns.enableQueue( hostQueue ) )
			_otherConns.enableQueue( hostQueue );
	}
	
	void addFavoredQueue( std::string hostQueue )
	{
		_favoredConns.addQueue( hostQueue );
	}
	
	void addOtherQueue( std::string hostQueue )
	{
		_otherConns.addQueue( hostQueue );
	}
	
	void deleteQueue( std::string hostQueue )
	{
		if( !_favoredConns.deleteQueue( hostQueue ) )
			_otherConns.deleteQueue( hostQueue );
	}

};


#endif  // CONNECTIONPOOLING_H


