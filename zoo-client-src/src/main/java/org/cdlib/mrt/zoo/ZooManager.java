/*
Copyright (c) 2011, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************/
package org.cdlib.mrt.zoo;


import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.core.ServiceStatus;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;


/**
 * Basic manager for Queuing Service
 * @author mreyes
 */
public class ZooManager
{

    private static final String NAME = "ZooManager";
    private static final String MESSAGE = NAME + ": ";
    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;
    //private Properties ingestProperties = null;
    private Properties queueProperties = null;
    private String queueConnectionString = null; //"hokusai.cdlib.org:2181";
    private String queueNode = null; //"/distrib.hokusai.1";
    //private String zooBase = null;
    private ArrayList<String> m_admin = new ArrayList<String>(20);

    private ZooKeeper zooKeeper = null;
    private ServiceStatus zooStatus = ServiceStatus.unknown;
    
    //private DistributedQueue distributedQueue = null;

    public static ZooManager getZooManager(LoggerInf logger, Properties conf)
        throws TException
    {
        try {
            ZooManager zooManager = new ZooManager(logger, conf);
            return zooManager;

        } catch (TException tex) {
	    throw tex;
        } catch (Exception ex) {
            String msg = MESSAGE + "QueueManager Exception:" + ex;
            logger.logError(msg, LoggerInf.LogLevel.SEVERE);
            logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex),
                    LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( msg);
        }
    }
    
    protected ZooManager(LoggerInf logger, Properties conf)
        throws TException
    {
	try {
            this.logger = logger;
            this.conf = conf;
            setStartupZoo();
	} catch (TException tex) {
            tex.printStackTrace();
	    throw tex;
	}
    }

    /**
     * <pre>
     * Initialize the QueueManager
     * Using a set of Properties identify all storage references.
     *
*!!!! -------------- May defer to use store node/node ID housed in Profile definitions ------ !!!!
     * Properties:
     * "Storage.nnn=value" identifies a storage reference. The nnn is the numeric serviceID.
     * The value is either a file name (local) or is a URL (remote)
     *
     * "IDDefault=" is the default serviceID used for accessing a storage service.
     * ingest -> ingest-info.txt - primarily the Access-uri is used in the  getVersion link manifest
     *
     * </pre>
     * @param prop system properties used to resolve Storage references
     * @throws TException process exceptions
     */
    private void setStartupZoo()
        throws TException
    {
        try {
            if (conf == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Exception MFrame properties not set");
            }

            String key = null;
            String value = null;
            String matchQueueService = "QueueService";
            String matchQueueNode = "QueueName";
            String matchAdmin = "admin";


	    // Queue and Ingest properties (ingest_info.txt/queue.txt)
            Enumeration e = conf.propertyNames();
            while( e.hasMoreElements() ) {
                key = (String) e.nextElement();
                value = conf.getProperty(key);
                if (key.equals(matchQueueService)) {
		    this.queueConnectionString = value;
                }
                if (key.equals(matchQueueNode)) {
		    this.queueNode = value;
                }

                // admin notification
                if (key.startsWith(matchAdmin)) {
                    for (String recipient : value.split(";")) {
                        m_admin.add((String) recipient);
                    }
                }
	    }
            if (DEBUG) System.out.println("Parms" + NL
                    + " - " + PropertiesUtil.dumpProperties("zooparm", queueProperties) + NL
                    + " - queueConnectionString=" + queueConnectionString + NL
                    + " - queueNode=" + queueNode + NL
                    );
            setZoo();
            DistributedQueue dummyQueue = getQueue();
            zooStatus = ServiceStatus.running;

        } catch (TException tex) {
	    throw tex;
        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
    
    
    public void setZoo()
        throws TException
    {
        try {
            closeZooKeeper();
            if (StringUtil.isEmpty(queueConnectionString)) {
                throw new TException.INVALID_OR_MISSING_PARM("ZooManager - queueConnectionString missing");
            }
            System.out.println("**Establishing new ZooKeeper:" + queueConnectionString);
            zooKeeper = new ZooKeeper(queueConnectionString, 300000, new Ignorer());

        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
    
    public DistributedQueue getQueue()
        throws TException
    {
        if (zooStatus == ServiceStatus.shutdown) return null;
        Exception zooException = null;
        DistributedQueue localDistributedQueue = null;
        for (int i=0; i<3; i++) {
            try {
                if (StringUtil.isEmpty(queueNode)) {
                    throw new TException.INVALID_OR_MISSING_PARM("ZooManager - queueNode not provided");
                }
                localDistributedQueue = new DistributedQueue(zooKeeper, queueNode, null);
                return localDistributedQueue; // default priority

            } catch (Exception ex) {
                zooException = ex;
                processZooException(ex);
                continue;
            }
        }
        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(zooException);
    }
    
    public void processZooException(Exception ex)
        throws TException
    {
        if (ex instanceof KeeperException) {
            setZoo();
            System.out.println(MESSAGE + "zookeeper SessionExpiredException exception:" + ex);
            return ;
        }
        else {
            ex.printStackTrace();
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(ex);
        }
    }
    
    private void closeZooKeeper()
    {
        try {
            if (zooKeeper != null) zooKeeper.close();
        } catch (Exception ze) {}
        zooKeeper = null;
    }
    
    public void startup()
        throws TException
    {
        if (zooKeeper != null) return;
        setZoo();
        DistributedQueue dummyQueue = getQueue();
        zooStatus = ServiceStatus.running;
    }
    
    public void shutdown()
        throws TException
    {
        zooStatus = ServiceStatus.shutdown;
        closeZooKeeper();
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
    
    public String getQueueBaseNode(String base)
    {
        if (StringUtil.isEmpty(base)) base = "";
        return queueNode + base;
    }
    
    public String dump(String header, byte[] bytes, Properties[] rows)
        throws TException
    {
        StringBuffer buf = new StringBuffer(2000);
        try {
            buf.append("***" + header + "***" + NL);
            if ((bytes != null) && (bytes.length > 0)) {
                buf.append("********** bytes ************" + NL);
                String out = new String(bytes, "utf-8");
                buf.append(out + NL);
            }
            if ((rows != null) && (rows.length > 0)) {
                buf.append("********** rows ************" + NL);
                for (int i=0; i< rows.length; i++) {
                    buf.append(PropertiesUtil.dumpProperties("(" + i + "):", rows[i]) + NL);
                }
            }
            buf.append(NL +  "*********************************************" + NL);
            return buf.toString();
            
            
        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
    
    public Properties getQueueServiceProps() {
        return queueProperties;
    }

    public String getZooNode() {
        return queueNode;
    }

    public String getQueueConnectionString() {
        return queueConnectionString;
    }
    

    public ServiceStatus getZookeeperStatus() {
        return zooStatus;
    }
    
    
   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }

    public LoggerInf getLogger() {
        return logger;
    }
    
    public void setStatusShutdown()
    {
        zooStatus = ServiceStatus.shutdown;
    }
   
}
