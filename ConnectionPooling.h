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
        this->start( );
    }
	
    static void sleep( int ms )
    {
        TaskThreadRunner::sleep(ms);
    }
};




template <class connType, class Locker, class LockPolicy > 
class ConnectionQueue
{
    std::queue< connType* > _Q;
    Locker _mx;
    bool _enable_state;
    double _time;

    public:

    ConnectionQueue( bool enabled = true )
        : _enable_state( enabled ), _time(0.0)  {}

    inline void pushConnection( connType* pConn, double alive_time = 0.0 )
    {
        LockPolicy  scopelock(_mx);
        _Q.push( pConn );

	if( alive_time > 0.0 )
		_time = alive_time;

    }

    inline connType* popConnection()
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

    int size()
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

    double alive_time()
    {
        return _time;
    }

};



template < class connType, class Locker, class LockPolicy, class TaskThreadRunnner > 
class ConnectionLoadBalancer
{
    Locker _mx;

    // No locking policies requested here
    typedef ConnectionQueue< connType, NoLock< bool >, NoLocking< NoLock< bool > > > ConnQue;

    // Locking requested policies here
    // typedef ConnectionQueue< connType, Lock< mutexType >, ScopeLevelLocking< Lock< mutexType > > > ConnQue;


    ThisThreader< ConnectionLoadBalancer, TaskThreadRunnner > 
        keepalive_threader, recycler_threader, disabler_threader; 

    // Host name based lookup connection pool map typedef
    typedef std::map< std::string, ConnQue > MapLB;
    typedef typename MapLB::iterator ItLB;
    
    ConnQue _recycling_bin;
    ConnQue _disable_bin;

    MapLB _mapLB;
    ItLB  _itLB;

public:

    ConnectionLoadBalancer() 
	: keepalive_threader( this, &ConnectionLoadBalancer::keepalive )
        , recycler_threader( this, &ConnectionLoadBalancer::recycler )
        , disabler_threader( this, &ConnectionLoadBalancer::disabler )
    {
        _itLB = _mapLB.begin();
    }

    ~ConnectionLoadBalancer()
    {
    }

    inline connType* popConnection(const char* szHost = NULL)
    {
        connType* pConn = NULL;
        LockPolicy  scopelock( _mx );

        //if( _mapLB.empty() ) // prevent a nasty loop when there are not Remotes
        //    return NULL;

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
    inline bool pushConnection(connType* pConn, double alive_time = 0.0 )
    {
        LockPolicy  scopelock(_mx);

        typename MapLB::iterator mapIt = _mapLB.find( pConn->host() );

        if( mapIt != _mapLB.end() )
        {
            if( mapIt->second.disabled() )
            {
                alive_time = 0.0;
                closeConnection<connType>( pConn );
            }
            mapIt->second.pushConnection( pConn, alive_time );
			return true;
        }
	else
	{
            return false;
	}
    }

    void pushRecycle(connType* pConn )
    {
        LockPolicy  scopelock(_mx);
        _recycling_bin.pushConnection( pConn );
    }

    bool disableQueue( std::string host)
    {
        LockPolicy  scopelock(_mx);

        typename MapLB::iterator mapIt = _mapLB.find( host );

        if( mapIt != _mapLB.end() )
        {
            mapIt->second.disable();
 
            connType *pConn;

            // Push these into the recycle bin to be disconnected reinserted
            while( ( pConn = mapIt->second.popConnection() ) != NULL )
               pushRecycle( pConn );       
 
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

            // Push these into the recycle bin to be disconnected reinserted
            connType *pConn;
            while( ( pConn = mapIt->second.popConnection() ) != NULL )
               pushRecycle( pConn );

            return true;
        }
	else
	{
		return false;
	}
    }

    void enableAll()
    {
	LockPolicy  scopelock(_mx);

	typename MapLB::iterator mapIt = _mapLB.begin();

	for( ; mapIt != _mapLB.end(); mapIt++ )
		mapIt->second.enable();
    }

    string queueStatusReport()
    {
        LockPolicy  scopelock(_mx);

        string report;

        char buf[255];

        typename MapLB::iterator mapIt = _mapLB.begin();

        for( ; mapIt != _mapLB.end(); mapIt++ )
        {
                if( !report.empty() )
                   report += "|";
		report += mapIt->first;
                report += ":";
                report += mapIt->second.enabled() ? "enabled" : "disabled";
                sprintf( buf, ":%d:%0.3f", mapIt->second.size(), mapIt->second.alive_time() );
                report += buf;
        }
 
        return report;
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

    // TODO: This should be done in a trashBin thread!!!
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
                                        closeConnection<connType>( pConn );
					delete pConn;
				}
			}
			
			_mapLB.erase( it );

			_itLB = _mapLB.begin(); // reset the rotor
			
			return true;
		}

		return false;
    }

    void start() 
    { 
	keepalive_threader.run();
	recycler_threader.run();
	disabler_threader.run();
    } 

 private:

    void keepalive()
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
		double alive_time = keepConnAlive<connType>( pConn );
		
		{   // Scope Locked
			LockPolicy scope( _mx );

			if( alive_time < 0.0 )
				pushRecycle( pConn );
			else
				pushConnection( pConn, alive_time );
		}
            }

            ThisThreader< ConnectionLoadBalancer, TaskThreadRunnner >::sleep( 1000 );
        }
    }
	
    void recycler()
    {
	while(true)
        {
            connType *pConn = NULL;

            {
               LockPolicy  scopelock(_mx);
               pConn = _recycling_bin.popConnection();
            } 
             
            if( recycleConnection<connType>( pConn ) )
            {
                double alive_time = keepConnAlive<connType>( pConn );
                {
                    LockPolicy scope( _mx );
                        
                    if( alive_time < 0.0 )
                            pushRecycle( pConn );
                    else
                            pushConnection( pConn, alive_time );
                }

                ThisThreader< ConnectionLoadBalancer, TaskThreadRunnner >::sleep( 200 );
             
                continue;
            }
            else
            {
                LockPolicy scope( _mx );
                _recycling_bin.pushConnection( pConn ); 
            }

            ThisThreader< ConnectionLoadBalancer, TaskThreadRunnner >::sleep( 1000 );
        }
    }

    void disabler()
    {
        while(true)
        {
            connType *pConn = NULL;

            {
               LockPolicy  scopelock(_mx);
               pConn = _disable_bin.popConnection();
            }

            if( pConn != NULL )
            {
                closeConnection<connType>( pConn );
                pushConnection( pConn, 0.0 );

                ThisThreader< ConnectionLoadBalancer, TaskThreadRunnner >::sleep( 200 );

                continue;
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
	
	bool pushConnection( connType* conn )
	{
		if( !_favoredConns.pushConnection(conn) )
		{
			if( !_otherConns.pushConnection(conn) )
			{
				conn->close();
				delete conn;
				return false;
			}
		}
		
		return true;
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

        void enableAllQueues()
	{
		_favoredConns.enableAll();
		_otherConns.enableAll();
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

        string poolStatusReport()
        {
                string report;
		
		report += "favored{" + _favoredConns.queueStatusReport() + "};";
		report += "other{" + _otherConns.queueStatusReport() + "};";

                return report;
        }
	
	void start_pool_managers()
	{
		_favoredConns.start();
		_otherConns.start();
	}

};


#endif  // CONNECTIONPOOLING_H


