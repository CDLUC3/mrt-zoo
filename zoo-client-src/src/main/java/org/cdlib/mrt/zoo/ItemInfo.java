/*
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
**********************************************************/
package org.cdlib.mrt.zoo;

import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;


/**
 * This object imports the formatTypes.xml and builds a local table of supported format types.
 * Note, that the ObjectFormat is being deprecated and replaced by a single format id (fmtid).
 * This change is happening because formatName is strictly a description and has no functional
 * use. The scienceMetadata flag is being dropped because the ORE Resource Map is more flexible
 * and allows for a broader set of data type.
 * 
 * @author dloy
 */
public class ItemInfo
{
    private static final String NAME = "ItemInfo";
    private static final String MESSAGE = NAME + ": ";
    public enum Status {
        PENDING((byte)0), 
        CONSUMED((byte)1), 
        DELETED((byte)2), 
        FAILED((byte)3), 
        COMPLETED((byte)4), 
        ALL((byte)-1);
        
        protected final byte status;
        Status(byte status) {
            this.status = status;
        }
        
        public byte getByte() 
        {
            return this.status;
        }

        public static Status statusFromByte(byte t)
        {
            for (Status p : Status.values()) {
                if (p.getByte() == t) {
                    return p;
                }
            }
            return null;
        }
    }
    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;
    
    protected Properties prop = null;
    protected String id = null;
    protected String distribBase = null;
    protected DateState date = null;
    protected Status status = Status.PENDING;
    protected Item item = null;
    

    
    public static ItemInfo getItemInfo(String base, String id, Item item)
        throws TException
    {
        try {
            ItemInfo info = new ItemInfo();
            info.setWithItem(base, id, item);
            return info;
            
        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
    public static ItemInfo getItemInfo(String base, String id, byte[] zookeeperBytes)
        throws TException
    {
        try {
            if (DEBUG) {
                String dump = new String(zookeeperBytes, "utf-8");
                System.out.println("Dump1" + dump);
            }
            Item item = Item.fromBytes(zookeeperBytes, id);
            if (item == null) {
                throw new TException.INVALID_OR_MISSING_PARM("getItemInfo - item is null");
            }
            ItemInfo info = new ItemInfo();
            info.setItem(item);
            byte[] bytes = item.getData();
            if (DEBUG) {
                String dump = new String(bytes, "utf-8");
                System.out.println("Dump2:" + dump);
            }
            info.setDistribBase(base);
            info.setProp(bytes);
            info.setDate(item.getTimestamp());
            info.setStatus(item.getStatus());
            info.setId(item.getId());
            return info;
            
            
        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
    public static ItemInfo getItemInfo(String distribBase, Status status, Properties prop)
        throws TException
    {
        try {
            ItemInfo info = new ItemInfo();
            info.setDistribBase(distribBase);
            info.setStatus(status);
            info.setProp(prop);
            return info;
            
            
        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
    
    public ItemInfo()
        throws TException
    {
        
    }
    
    public Item buildNewItem()
        throws TException
    {
        if (prop.size() == 0) {
            throw new TException.INVALID_OR_MISSING_PARM("buildNewItem - propList required");
        }
        byte[] data = ZooUtil.encodeItem(prop);
        if (status == null) {
            throw new TException.INVALID_OR_MISSING_PARM("buildNewItem - status required");
        }
        if (date == null) {
            date = new DateState();
        }
        if (DEBUG) {
            try {
            System.out.println("buildNewItem:\n"
                    + " - data:" + new String(data, "utf-8")
                    + " - status:" + status.getByte() + "\n"
                    + " - date:" + date + "\n"
                    );  
            } catch (Exception e) { }
            
        } 
        Item item = new Item(status.getByte(), data, date.getDate(), id);
        
        if (DEBUG) {
            try {
                String dump = new String(item.getData(), "utf-8");
            System.out.println("buildNewItem:\n"
                    + " - item dump:" + dump
                    + " - item status:" + item.getStatus() + "\n"
                    + " - item getTimestamp:" + item.getTimestamp() + "\n"
                    );  
            } catch (Exception e) { }
            
        } 
        return item;
    }
    
    public void setWithItem(String base, String id, Item item)
        throws TException
    {
        try {
            if (item == null) {
                throw new TException.INVALID_OR_MISSING_PARM("getItemInfo - item is null");
            }
            setItem(item);
            byte[] bytes = item.getData();
            setDistribBase(base);
            setProp(bytes);
            setDate(item.getTimestamp());
            setStatus(item.getStatus());
            if (id == null) {
                if (item.getId() != null) id = item.getId();
                else item.setId(id);
            }
            setId(id);
            
            
        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
    
    public Properties getProp() {
        return prop;
    }

    public void setProp(byte[] bytes)
        throws TException
    {       
        try {
            if ((bytes == null) || (bytes.length == 0)) return;
            prop = ZooUtil.decodeItem(bytes);
            
            
        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }

    public void setProp(Properties prop) {
        this.prop = prop;
    }
    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DateState getDate() {
        return date;
    }

    public void setDate(DateState date) {
        this.date = date;
    }

    public void setDate(Date dateD) {
        this.date = null;
        if (dateD != null) {
            date = new DateState(dateD);
        }
    }

    public String getDistribBase() 
        throws TException 
    {
        if (StringUtil.isAllBlank(this.distribBase)) {
            throw new TException.INVALID_OR_MISSING_PARM("getDistribBase requires distribBase");
        }
        return distribBase;
    }

    public void setDistribBase(String distribBase) 
        throws TException
    {
        if (StringUtil.isAllBlank(distribBase)) {
            throw new TException.INVALID_OR_MISSING_PARM("setDistribBase requires distribBase");
        }
        this.distribBase = ZooUtil.testBase(distribBase);
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setStatus(byte statusB) {
        this.status = Status.statusFromByte(statusB);
    }

    public Item retrieveItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }
    
    public String dump(String header) 
    {
        StringBuffer buf = new StringBuffer();
        buf.append("***" + header + "***" + NL
                + " - id=" + id + NL
                + " - distribBase=" + distribBase + NL
                );
        
        buf.append("***Properties:" + NL);
        buf.append(PropertiesUtil.dumpProperties("prop", prop) + NL);
        return buf.toString();
    }
    
}
