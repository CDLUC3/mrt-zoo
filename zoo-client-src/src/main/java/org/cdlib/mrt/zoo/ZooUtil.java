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




import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.ZooCodeUtil;

import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.utility.TException;

/**
 * Basic manager for Queuing Service
 * @author mreyes
 */
public class ZooUtil
    extends ZooCodeUtil
{

    private static final String NAME = "ZooUtil";
    private static final String MESSAGE = NAME + ": ";
    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = true;



   public static Properties decodeItem(Item item)
       throws TException
   {
        try {
            byte[] bytes = item.getBytes();
            if ((bytes == null) || (bytes.length == 0)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "decodeItem - missing bytes");
            }
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            Properties row = (Properties)decodeItem(bais);
            return row;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
   }
    
   public static void dumpItem(String queueDir, String headNode, Item item)
       throws TException
   {
       
            
        try {
            System.out.println("***********************************" + NL);
            System.out.println("QueueDir:" + queueDir + NL);
            System.out.println("Headnode:" + headNode + NL);
            System.out.println("Status:" + printStatus(item.getStatus()) + NL);
            
            byte[] bytes = item.getData();
            String out = new String(bytes, "utf-8");
            System.out.println(out);
            System.out.println("***********************************" + NL);

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
   }
   
   public static void dumpItemProperties(String queueDir, String headNode, Item item)
       throws TException
   {
       String dump = dumpStringItemProperties(queueDir, headNode, item);
       System.out.println(dump);
   
   }
   
   public static String dumpStringItemProperties(String queueDir, String headNode, Item item)
       throws TException
   {
       
        StringBuilder buf = new StringBuilder();
        try {
            buf.append(NL + "***********************************" + NL);
            buf.append(NL + "QueueDir:" + queueDir);
            if (headNode != null) buf.append(NL + "Headnode:" + headNode);
            if (item.getId() != null) buf.append(NL + "Id:" + item.getId());
            buf.append(NL + "Status:" + printStatus(item.getStatus()) + NL);
            
            byte[] bytes = item.getData();
            String out = new String(bytes, "utf-8");
            buf.append(NL + out);
            buf.append(NL + "------------Properties-------------" + NL);
            Properties row = decodeItem(bytes);
            buf.append(NL + PropertiesUtil.dumpProperties("Row", row));
            return buf.toString();
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
   
   }
   
}
