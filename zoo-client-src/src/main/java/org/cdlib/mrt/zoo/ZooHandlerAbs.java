/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
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
*******************************************************************************/
package org.cdlib.mrt.zoo;



import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.core.ServiceStatus;
import org.cdlib.mrt.core.ProcessStatus;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public abstract class ZooHandlerAbs
        implements Runnable
{

    protected static final String NAME = "ZooHandlerAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final boolean DEBUG_EX = true;
    protected static final boolean STATUS = true;
    
    protected ZooQueue zooQueue = null;
    protected ZooManager zooManager = null;
    protected long pollTime = 30000;
    protected LoggerInf logger = null;
    protected int esuCnt = 0;
    protected Exception saveException = null;
    
 

    protected ZooHandlerAbs (
            ZooManager zooManager,
            long pollTime,
            LoggerInf logger)
        throws TException
    {
        this.zooManager = (ZooManager)notNull("zooManager", zooManager);
        this.logger = (LoggerInf)notNull("logger", logger);
        this.pollTime = pollTime;
        if (pollTime < 5000) pollTime = 30000;
        zooQueue = ZooQueue.getZooQueue(zooManager);
    }    
    
    public static Object notNull(String header, Object object)
        throws TException
    {
        if (object == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + header + " null");
        }
        return object;
    }

    public void run()
    {
        try {
            process();
            
        } catch (Exception ex) {
            saveException = ex;
            return;
        }
    }

    public void process()
        throws TException
    {
        int cleCnt = 1;
        for (long ijob=0; true; ijob++) {
            if (zooManager.getZookeeperStatus() == ServiceStatus.shutdown) {
                break;
            }
            DistributedQueue queue = zooQueue.getQueue();
            if (queue == null) return;
            Item item = null;
            try { 
                
                try {
                    item = queue.consume();
                    if (STATUS) System.out.println("ZooHandler consume(" + ijob + "):"
                            + " - item:" + item.getId()
                            );
                    cleCnt = 0;
                    
                } catch (java.util.NoSuchElementException nsee) {
                    if (STATUS) System.out.println("ZooHandler sleep:" + pollTime);
                    Thread.sleep(pollTime);
                    if (DEBUG) System.out.println("consume continue");
                    continue;
                }
                ProcessStatus status = processItem(ijob, item);
                if (DEBUG) System.out.println("ZooHandler Status:" + status.toString());
                if ((status == ProcessStatus.unknown) || (status == ProcessStatus.shutdown)) {
                    System.out.println("Item requeued");
                    queue.requeue(item);
                }
                esuCnt = 0;
                
            } catch (org.apache.zookeeper.KeeperException ke) {
                ke.printStackTrace();
                if (zooManager.getZookeeperStatus() == ServiceStatus.shutdown) {
                    shutdown();
                    break;
                }
                if (cleCnt > 20) {
                    handleException(ke, item);
                }
                int mult = 1;
                for (int m=0; m<=cleCnt; m++) mult *= 2;
                long sleepTime = mult * 15000;
                System.out.println(MESSAGE + "WARNING - KeeperException restart attempt("
                        + cleCnt + "):" + sleepTime
                        + "Exception:" + ke
                        );
                try {
                    Thread.sleep(sleepTime);
                } catch (Exception se) { }
                try {
                    cleCnt++;
                    zooQueue.processException(ke);
                    System.out.println("Restart attempt appears successful");
                    continue;
                } catch (TException tex) {
                    System.out.println("Restart attempt fails:" + tex);
                    tex.printStackTrace();
                }
                continue;
                
            } catch (Exception ex) {
                handleException(ex, item);
            }
        }
    }
    
    public void shutdown()
        throws TException
    {
        try {
            //may be overriddent
            
        } catch (Exception ex) {
        }
    }
    
    protected void handleException(Exception passedException, Item item)
        throws TException
    {
        if (DEBUG_EX) {
            System.out.println("INFO ZooHandlerAbs-Exception:" + passedException);
            passedException.printStackTrace();
        }
        try {
            if (zooManager.getZookeeperStatus() == ServiceStatus.shutdown) {
                shutdown();
                return;
            }
            DistributedQueue queue = zooManager.getQueue();
            
            if (passedException instanceof TException.REQUEST_ITEM_EXISTS) {
                return;
            }
            queue.requeue(item);
            if (passedException instanceof TException.EXTERNAL_SERVICE_UNAVAILABLE) {
                esuCnt++;
                if (esuCnt >= 3) {
                    logger.logError("External Service Unavailable count excceeded:" 
                            + " - esuCnt=" + esuCnt
                            + " - Exception:" + passedException.toString()
                            , 0);
                    throw (TException.EXTERNAL_SERVICE_UNAVAILABLE)passedException;
                    
                } else {
                    try {
                        if (DEBUG) System.out.println("Unavailable service sleep: 7200 sec");
                        Thread.sleep(7200000); // 2hours
                        return;

                    } catch (Exception ex) {
                        logger.logMessage("SLEEP Exception: " + ex, 0, true);
                    }
                }
                    
            }
            if (passedException instanceof TException) {
                logger.logError("TException:" + passedException, 0);
                logger.logError("Trace:" + StringUtil.stackTrace(passedException), 5);
                throw (TException)passedException;
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected abstract ProcessStatus processItem(
            long ijob,
            Item item)
        throws TException;
    
    public void removeItem(Item item)
    {
        try {
            logger.logError("Item miss:" + dumpItem(item), 0);
            DistributedQueue queue = zooQueue.getQueue();
            queue.delete(item.getId());
            return;
            
            
        } catch (Exception ex) {
            logger.logMessage("Warning - exception during delete:" + ex, 0);
        }
    }
    
    protected String dumpItem(Item item)
    {   
        try {
            if (item == null) return "Item null";
            return ZooUtil.dumpStringItemProperties(zooQueue.getZooNode(), MESSAGE + "dumpItem", item);
            
        } catch (Exception ex) {
            return "Exception:" + ex;
        }
        
    }
}

