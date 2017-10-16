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
import java.util.Properties;
import java.util.TreeMap;


import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;


/**
 * Basic manager for Queuing Service
 * @author mreyes
 */
public class ZooQueue
{

    private static final String NAME = "ZooQueue";
    private static final String MESSAGE = NAME + ": ";
    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;
    private LoggerInf logger = null;

    private ZooManager zooManager = null;
    private DistributedQueue queue = null;
    //private String zooBase = null;
    
    //private DistributedQueue distributedQueue = null;

    public static ZooQueue getZooQueue(ZooManager zooManager)
        throws TException
    {
        try {
            return new ZooQueue(zooManager);

        } catch (TException tex) {
	    throw tex;
        } catch (Exception ex) {
            String msg = MESSAGE + "ZooQueue Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION( msg);
        }
    }
    
    protected ZooQueue(ZooManager zooManager)
        throws TException
    {
	try {
            this.zooManager = zooManager;
            if (zooManager == null) throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing zooManager");
            logger = zooManager.getLogger();
            queue = zooManager.getQueue();
            
            
	} catch (TException tex) {
	    throw tex;
	}
    }
    
    public void processException(Exception pex)
        throws TException
    {
        zooManager.processZooException(pex);
        queue = zooManager.getQueue();
    }
    
    public ItemInfo getInfo(Item item)
       throws TException
    {
        if (item == null) return null;
        return ItemInfo.getItemInfo(getZooNode(), null, item);        
    }
    
    public ArrayList<ItemInfo> browse(String base, byte status, int maxCnt)
        throws Exception
    {
        ArrayList<ItemInfo> propList = new ArrayList();
        if (DEBUG) System.out.println("browseit - search status=" + status);
        try {
            DistributedQueue distributedQueue = queue;
            ZooKeeper zooKeeper = zooManager.getZooKeeper();

            TreeMap<Long,String> orderedChildren;
            try {
                orderedChildren = distributedQueue.orderedChildren(null);
            } catch(KeeperException.NoNodeException e){
                throw e;
            }
            for (String headNode : orderedChildren.values()) {
                String path = String.format("%s/%s", distributedQueue.dir, headNode);
                try {
                    byte[] bytes = zooKeeper.getData(path, false, null);
                    if (DEBUG) {
                        try {
                            
                            System.out.println("path=" + path + " - size=" + bytes.length);
                            
                            System.out.println("ZooQueue:" + new String(bytes, "utf-8"));
                        } catch (Exception e) { }
                    }
                    ItemInfo info = ItemInfo.getItemInfo(base, path, bytes);
                    if (DEBUG) {
                        System.out.println("browseit PATH:" + path 
                            + " - status=" + info.getStatus());
                        Properties dumpProp = info.getProp();
                        System.out.println(PropertiesUtil.dumpProperties("browse", dumpProp));
                    }
                            
                    byte matchByte = info.getStatus().getByte();
                    if ((status < 0) || (matchByte == status)) {
                            propList.add(info);
                    }
                    if (propList.size() >= maxCnt) break;

                } catch(KeeperException.NoNodeException e){
                    throw e;
                }
            }
            return propList;
            
        } catch (Exception ex) {
            throw ex;
        }
    }
    
    public LoggerInf getLogger() {
        return logger;
    }

    public ZooManager getZooManager() {
        return zooManager;
    }

    public DistributedQueue getQueue() {
        return queue;
    }

    public String getZooNode() {
        return zooManager.getZooNode();
    }
}
