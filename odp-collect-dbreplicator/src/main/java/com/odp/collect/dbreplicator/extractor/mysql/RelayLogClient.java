/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): Gilles Rayrat, Stephane Giron
 */

package com.odp.collect.dbreplicator.extractor.mysql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.io.WrappedInputStream;
import com.continuent.tungsten.common.mysql.MySQLConstants;
import com.continuent.tungsten.common.mysql.MySQLIOs;
import com.continuent.tungsten.common.mysql.MySQLPacket;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.network.protocol.command.Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.DumpBinaryLogGtidCommand;
//import com.odp.collect.dbreplicator.GtidSet;

/**
 * Defines a client to extract binlog events and store them in local relay files
 * in a fashion similar to MySQL.
 * <p>
 * Public methods are synchronized to ensure a consistent view of client data
 * across threads.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class RelayLogClient
{
    private static Logger             logger                      = Logger.getLogger(RelayLogClient.class);

    // Magic number for MySQL binlog files.
    private static byte[]             magic                       = {
            (byte) 0xfe, 0x62, 0x69, 0x6e                         };

    // Tells the server that we can handle MARIA GTID event
    private static int                MARIA_SLAVE_CAPABILITY_GTID = 4;

    // Options.
    private String                    url                         = "jdbc:mysql:thin://localhost:3306/";
    private String                    login                       = "tungsten";
    private String                    password                    = null;
    private String                    binlog                      = null;
    private String                    binlogPrefix                = "mysql-bin";
    private long                      offset                      = 4;
    private String                    binlogDir                   = ".";
    private boolean                   autoClean                   = true;
    private int                       serverId                    = 1;
    private long                      readTimeout                 = 60;
    private boolean                   deterministicIo             = false;
    private LinkedBlockingQueue<File> logQueue                    = null;

    // Relay storage and positioning information.
    private File                      relayLog;
    private File                      relayDir;
    private File                      binlogIndex;
    private OutputStream              relayOutput;
    private long                      relayBytes;
    private RelayLogPosition          logPosition                 = new RelayLogPosition();

    // Database connection information.
    private Connection                conn;
    private InputStream               input                       = null;
    private OutputStream              output                      = null;
    private String                    checksum                    = null;
    private GtidSet                   gtidSet 					  = null;
    private int						  binlogSeq					  = 0;

    /** Create new relay log client instance. */
    public RelayLogClient()
    {
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getLogin()
    {
        return login;
    }

    public void setLogin(String login)
    {
        this.login = login;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getBinlog()
    {
        return binlog;
    }

    public void setBinlog(String binlog)
    {
        this.binlog = binlog;
    }
    
    public void setBinlog(String binlog, long serverId, int binlogSeq){
    	this.binlog = binlog + "." + serverId + "." + binlogSeq;
    }

    public String getBinlogPrefix()
    {
        return binlogPrefix;
    }

    public void setBinlogPrefix(String binlogPrefix)
    {
        this.binlogPrefix = binlogPrefix;
    }

    public long getOffset()
    {
        return offset;
    }

    public void setOffset(int offset)
    {
        this.offset = offset;
    }

    public String getBinlogDir()
    {
        return binlogDir;
    }

    public void setBinlogDir(String out)
    {
        this.binlogDir = out;
    }

    public boolean isAutoClean()
    {
        return autoClean;
    }

    public void setAutoClean(boolean autoClean)
    {
        this.autoClean = autoClean;
    }

    public void setServerId(int serverId)
    {
        this.serverId = serverId;
    }

    public void setDeterministicIo(boolean deterministicIo)
    {
        this.deterministicIo = deterministicIo;
    }

    public synchronized LinkedBlockingQueue<File> getLogQueue()
    {
        return logQueue;
    }

    public synchronized void setLogQueue(LinkedBlockingQueue<File> logQueue)
    {
        this.logQueue = logQueue;
    }

    /**
     * Returns the network read delay timeout.
     */
    public synchronized long getReadTimeout()
    {
        return readTimeout;
    }

    /**
     * Set the number of seconds delay we permit before giving up on partial
     * reads from the network.
     */
    public synchronized void setReadTimeout(long readTimeout)
    {
        this.readTimeout = readTimeout;
    }

    /** Connect to MySQL and start pulling down data. */
    public static void main(String[] args)
    {
        RelayLogClient relayClient = new RelayLogClient();

        // Process command line arguments.
        String curArg = null;
        try
        {
            int argIndex = 0;
            while (argIndex < args.length)
            {
                curArg = args[argIndex++];
                if ("-url".equals(curArg))
                    relayClient.setUrl(args[argIndex++]);
                else if ("-login".equals(curArg))
                    relayClient.setLogin(args[argIndex++]);
                else if ("-password".equals(curArg))
                    relayClient.setPassword(args[argIndex++]);
                else if ("-binlog".equals(curArg))
                    relayClient.setBinlog(args[argIndex++]);
                else if ("-offset".equals(curArg))
                    relayClient.setOffset(new Integer(args[argIndex++]));
                else if ("-binlogdir".equals(curArg))
                    relayClient.setBinlogDir(args[argIndex++]);
                else if ("-autoclean".equals(curArg))
                    relayClient.setAutoClean(new Boolean(args[argIndex++]));
                else if ("-help".equals(curArg))
                {
                    printUsage();
                    System.exit(0);
                }
                else
                {
                    System.out.println("Unrecognized option: " + curArg);
                    printUsage();
                    System.exit(1);
                }
            }
        }
        catch (Exception e)
        {
            System.out.println("Invalid or missing data for argument: "
                    + curArg);
            printUsage();
            System.exit(1);
        }

        // Connect and process data.
        try
        {
            relayClient.connect();
            while (true)
                relayClient.processEvent();
        }
        catch (Exception e)
        {
            logger.fatal(
                    "Relay client failed with unexpected exception: "
                            + e.getMessage(), e);
        }
        finally
        {
            relayClient.disconnect();
        }
        logger.info("Done!");
    }

    // Print help for command line utility.
    private static void printUsage()
    {
        System.out.println("Usage: BinlogRelayClient2 options");
        System.out.println("Options:");
        System.out.println("  -host <host>         Default: localhost");
        System.out.println("  -port <port>         Default: 3306");
        System.out.println("  -login <login>       Default: tungsten");
        System.out.println("  -password <password> Default: secret");
        System.out.println("  -binlog <binlog>     Default: (1st binlog file)");
        System.out.println("  -offset <offset>     Default: 0");
        System.out.println("  -binlogdir <dir>     Default: current directory");
        System.out.println("  -autoclean <true|false> Default: true");
        System.out.println("  -help                Print help message");
    }

    /**
     * Connect to database and set up relay log transfer. If successful we are
     * ready to transfer binlogs.
     */
    public void connect() throws ReplicatorException
    {
        try
        {
            logger.info("Connecting to master MySQL server: url=" + url);
            Class.forName("org.drizzle.jdbc.DrizzleDriver");
            conn = DriverManager.getConnection(url, login, password);
        }
        catch (ClassNotFoundException e)
        {
            throw new ExtractorException("Unable to load JDBC driver", e);
        }
        catch (SQLException e)
        {
            throw new ExtractorException("Unable to connect", e);
        }

        // Get underlying IO streams for network communications.
        try
        {
            MySQLIOs io = MySQLIOs.getMySQLIOs(conn);
            input = new WrappedInputStream(io.getInput(), deterministicIo);
            output = io.getOutput();
        }
        catch (Exception e)
        {
            throw new ExtractorException(
                    "Unable to access IO streams for connection", e);
        }

        // Set up output directories for relay logs.
        this.relayDir = new File(binlogDir);
        if (!relayDir.isDirectory())
            throw new ExtractorException(
                    "Relay log directory not a directory or does not exist: "
                            + relayDir.getAbsolutePath());
        else if (!relayDir.canWrite())
            throw new ExtractorException(
                    "Relay log directory is not writable: "
                            + relayDir.getAbsolutePath());

        // Compute binlog index file.
        binlogIndex = new File(relayDir, binlogPrefix + ".index");

        // If auto-clean is enabled, clean up relay files now.
        if (autoClean)
        {
            // Delete the index file if it exists.
            if (binlogIndex.delete())
                logger.info("Cleaned up binlog index file: "
                        + binlogIndex.getAbsolutePath());

            // Delete any binlog file equal to or greater than the requested
            // binlog file. If no binlog is requested, delete all of them.
            String baseLog;
            if (this.binlog == null)
                baseLog = "";
            else
                baseLog = binlog;

            for (File child : relayDir.listFiles())
            {
                // Find binlogs that sort higher than the requested file.
                if (!child.isFile())
                    continue;
                else if (!child.getName().startsWith(this.binlogPrefix))
                    continue;
                else if (child.getName().compareTo(baseLog) < 0)
                    continue;

                if (child.delete())
                    logger.info("Cleaned up binlog file: "
                            + child.getAbsolutePath());
            }
        }

        Statement statement = null;
        try
        {
            // Tell master we are checksum aware
            statement = conn.createStatement();
            statement
                    .executeUpdate("SET @master_binlog_checksum= @@global.binlog_checksum");

            // And check whether master is going to send checksum events
            ResultSet rs = statement
                    .executeQuery("SELECT @@binlog_checksum as checksum");

            if (rs.next())
            {
                checksum = rs.getString("checksum");
                logger.info("Master binlog is checksummed with : " + checksum);
            }
            rs.close();
        }
        catch (SQLException e1)
        {
            if (logger.isDebugEnabled())
                logger.debug("This server does not support checksums", e1);
            else
                logger.info("This server does not support checksums");
        }
        finally
        {
            if (statement != null)
                try
                {
                    statement.close();
                }
                catch (SQLException e)
                {
                }
        }

        try
        {
            // Tell master we know about MariaDB 10 GTID events
            statement = conn.createStatement();
            statement.executeUpdate("SET @mariadb_slave_capability="
                    + MARIA_SLAVE_CAPABILITY_GTID);
            
            
            
            //czq add
            // check whether mstart set @mariadb_slave_capability to MARIA_SLAVE_CAPABILITY_GTID
            
            //statement.executeUpdate("SET @mariadb_slave_capability=4");
            ResultSet rs = statement
                    .executeQuery("SELECT @mariadb_slave_capability");

            if (rs.next())
            {
                String  capability = rs.getString("@mariadb_slave_capability");
                logger.info("Master @mariadb_slave_capability is: " + capability);
            }
            rs.close();
            //czq end
        }
        catch (SQLException e1)
        {
            logger.info("Failure while setting MariaDB 10 GTIDs support");
        }
        finally
        {
            if (statement != null)
                try
                {
                    statement.close();
                }
                catch (SQLException e)
                {
                }
        }

        // Ask for binlog data.
        logger.info("Binlog read timeout set to " + readTimeout + " seconds");
        try
        {
            //offset固定是 4？
            logger.info("Requesting binlog data from master: " + binlog + ":"
                    + offset);
            //sendBinlogDumpPacket(output);
            sendGtidDumpPacket(output);
        }
        catch (IOException e)
        {
            throw new ExtractorException(
                    "Error sending request to dump binlog", e);
        }
    }

    /**
     * Process next event packet from MySQL.
     * 
     * @return True if a packet was processed, otherwise false. Latter case may
     *         indicate that the connection has been terminated.
     */
    public boolean processEvent() throws ReplicatorException,
            InterruptedException
    {
        MySQLPacket packet = MySQLPacket.readPacket(input, readTimeout * 1000);
        if (packet == null)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("No packet returned from network");
            }
            return false;
        }
        int length = packet.getDataLength();
        int number = packet.getPacketNumber();
        short type = packet.getUnsignedByte();

        if (logger.isDebugEnabled())
        {
            logger.debug("Received packet: number=" + number + " length="
                    + length + " type=" + type);
        }

        // Switch on the type.
        switch (type)
        {
            case 0 :
                // Indicates the next event. (MySQL dev wiki doc on connection
                // protocol are wrong or misleading.)
                try
                {
                    processBinlogEvent(packet);
                }
                catch (IOException e)
                {
                    throw new ExtractorException("Error processing binlog: "
                            + e.getMessage(), e);
                }
                break;
            case 0xFE :
                // Indicates end of stream. It's not clear when this would
                // be sent.
                throw new ExtractorException("EOF packet received");
            case 0xFF :
                // Indicates an error, for example trying to restart at wrong
                // binlog offset.
                int errno = packet.getShort();
                packet.getByte();
                String sqlState = packet.getString(5);
                String errMsg = packet.getString();

                String msg = "Error packet received: errno=" + errno
                        + " sqlstate=" + sqlState + " error=" + errMsg;
                throw new ExtractorException(msg);
            default :
                // Should not happen.
                throw new ExtractorException(
                        "Unexpected response while fetching binlog data: packet="
                                + packet.toString());
        }
        return true;
    }

    /**
     * Clean up after termination.
     */
    public void disconnect()
    {
        // Disconnect from database.
        if (conn != null)
        {
            try
            {
                conn.close();
            }
            catch (SQLException e)
            {
                logger.warn("Unable to close connection", e);
            }
        }

        // Close current binlog if there is one.
        try
        {
            closeBinlog();
        }
        catch (IOException e)
        {
            logger.warn("Unable to close binlog", e);
        }
    }

    /**
     * Returns the current relay log position.
     */
    public RelayLogPosition getPosition()
    {
        return logPosition.clone();
    }

    /**
     * Sends a binlog dump request to server.
     * 
     * @param out Output stream on which to write packet to server
     */
    private void sendBinlogDumpPacket(OutputStream out) throws IOException
    {
        MySQLPacket packet = new MySQLPacket(200, (byte) 0);
        packet.putByte((byte) MySQLConstants.COM_BINLOG_DUMP);
        packet.putInt32((int) offset);
        packet.putInt16(0);
        packet.putInt32(serverId);
        if (binlog != null)
            //packet.putString(binlog); //此处的binlog就是最开始从eventId中解析出来的binlog文件名
        	packet.putString("mysql-bin.000177");
        packet.write(out);
        out.flush();
    }
    
    /**
     * Sends a binlog dump gtid request to server.
     * 
     * @param out Output stream on which to write packet to server
     */
    private void sendGtidDumpPacket(OutputStream out) throws IOException {
        MySQLPacket packet = new MySQLPacket(200, (byte) 0);
        //packet.putByte((byte)MySQLConstants.COM_BINLOG_DUMP_GTID);
        //COM_BINLOG_DUMP_GTID
        /*packet.putByte((byte) 30);
        //flags
        packet.putInt16(0);
        //server-id
        packet.putInt32(serverId);
        //binlog-filename-len
        //packet.putInt32(binlog.length());
        //packet.putString(binlog);
        packet.putInt32(binlog.length());
        packet.putString(binlog);
        //binlog-pos
        packet.putLong((long)offset);
        
        Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        int dataSize = 8  number of uuidSets ;
        for (GtidSet.UUIDSet uuidSet : uuidSets) {
            dataSize += 16  uuid  + 8  number of intervals  +
                uuidSet.getIntervals().size()  number of intervals  * 16  start-end ;
        }
        //buffer.writeInteger(dataSize, 4);
        //buffer.writeLong(uuidSets.size(), 8);
        packet.putInt32(dataSize);
        packet.putLong((long)uuidSets.size());
        for (GtidSet.UUIDSet uuidSet : uuidSets) {
            //buffer.write(hexToByteArray(uuidSet.getUUID().replace("-", "")));
        	packet.putBytes(hexToByteArray(uuidSet.getUUID().replace("-", "")));
            Collection<GtidSet.Interval> intervals = uuidSet.getIntervals();
            //buffer.writeLong(intervals.size(), 8);
            packet.putLong(intervals.size());
            for (GtidSet.Interval interval : intervals) {
                //buffer.writeLong(interval.getStart(), 8);
                //buffer.writeLong(interval.getEnd() + 1 , 8);
            	packet.putLong(interval.getStart());
            	packet.putLong(interval.getEnd() + 1 );
            }
        }
		*/
        System.out.println("here");
        
        Command command = new DumpBinaryLogGtidCommand(serverId, binlog, 4, gtidSet);
        
        packet.putBytes(command.toByteArray());
        packet.write(out);
        //out.write(command.toByteArray());
        out.flush();
    }

    /**
     * Process a binlog event using light parsing to detect the binlog position,
     * timestamp, and the event type. We need to detect ROTATE_LOG events.
     * 
     * @param packet
     * @throws IOException
     * @throws InterruptedException
     */
    private void processBinlogEvent(MySQLPacket packet) throws IOException,
            InterruptedException
    {
        // Read the header. Note we can only handle V4 headers (5.0+).
        long timestamp = packet.getUnsignedInt32();
        int typeCode = packet.getUnsignedByte();
        long serverId = packet.getUnsignedInt32();
        long eventLength = packet.getUnsignedInt32();
        long nextPosition = packet.getUnsignedInt32();
        int flags = packet.getUnsignedShort();

        if (logger.isDebugEnabled())
        {
            StringBuffer sb = new StringBuffer("Reading binlog event:");
            sb.append(" timestamp=").append(timestamp);
            sb.append(" type_code=").append(typeCode);
            sb.append(" server_id=").append(serverId);
            sb.append(" event_length=").append(eventLength);
            sb.append(" next_position=").append(nextPosition);
            sb.append(" flags=").append(flags);
            if (logger.isDebugEnabled())
                logger.debug(sb.toString());
        }
        
        /*{
        	StringBuffer sb = new StringBuffer("Reading binlog event:");
            sb.append(" timestamp=").append(timestamp);
            sb.append(" type_code=").append(typeCode);
            sb.append(" server_id=").append(serverId);
            sb.append(" event_length=").append(eventLength);
            sb.append(" next_position=").append(nextPosition);
            sb.append(" flags=").append(flags);
            logger.info(sb.toString());
        }*/
        
        if (typeCode == MysqlBinlog.ROTATE_EVENT)
        {
            // Store ROTATE_EVENT data so that we open up a new binlog event.
            offset = packet.getLong();
            if (checksum != null && !checksum.equalsIgnoreCase("NONE"))
            {
                binlog = packet.getString(packet.getRemainingBytes() - 4);
            }
            else
            {
                binlog = packet.getString();
            }
            
            /* czq add */
            binlog = binlog + "." + serverId + "." + binlogSeq;
            
            if (logger.isDebugEnabled())
            {
                StringBuffer sb2 = new StringBuffer("ROTATE_EVENT:");
                sb2.append(" next_start_offset=").append(offset);
                sb2.append(" next_binlog_name=").append(binlog);
                if (logger.isDebugEnabled())
                    logger.debug(sb2.toString());
            }
            
            /*{
            	StringBuffer sb2 = new StringBuffer("ROTATE_EVENT:");
                sb2.append(" next_start_offset=").append(offset);
                sb2.append(" next_binlog_name=").append(binlog);
                logger.info(sb2.toString());
            }*/
            // Write rotate_log event only if we have an open relay log file.
            // MySQL also sends same event at the beginning of a new file.
            if (this.relayOutput != null)
            {
                writePacketToRelayLog(packet);
                closeBinlog();
            }
        }
        else
        {
            writePacketToRelayLog(packet);
        }
    }

    // Write a packet to relay log.
    private void writePacketToRelayLog(MySQLPacket packet) throws IOException,
            InterruptedException
    {
        if (relayOutput == null)
            openBinlog();
        blindlyWriteToRelayLog(packet, false);
        while (packet.getDataLength() >= MySQLPacket.MAX_LENGTH)
        {
            // this is a packet longer than 16m. Data will be send over several
            // packets so we need to read/write the next packets blindly until a
            // packet smaller than 16m is found
            packet = MySQLPacket.readPacket(input, readTimeout * 1000);
            if (logger.isDebugEnabled())
            {
                logger.debug("Read extended packet: number="
                        + packet.getPacketNumber() + " length="
                        + packet.getDataLength());
            }
            blindlyWriteToRelayLog(packet, true);
        }
    }

    /**
     * Writes data into the relay log file.
     * 
     * @param packet Network packet to be written
     * @param extended If false, this is a base packet with 5 byte header
     *            including type; if true, this is a follow-on packet with 4
     *            byte header (not including type field)
     * @throws IOException
     */
    private void blindlyWriteToRelayLog(MySQLPacket packet, boolean extended)
            throws IOException
    {
        byte[] bytes = packet.getByteBuffer();
        int header;
        // Header size affects math for start and length of written data.
        if (extended)
            header = 4;
        else
            header = 5;
        int writeLength = bytes.length - header;
        if (logger.isDebugEnabled())
        {
            logger.debug("Writing packet to binlog: bytesLength="
                    + bytes.length + " writeLength=" + writeLength);
        }
        relayOutput.write(bytes, header, writeLength);
        relayOutput.flush();
        relayBytes += writeLength;
        //logger.info("relayBytes:" + relayBytes);
        logPosition.setPosition(relayLog, relayBytes);
    }

    // Open a new binlog file.
    private void openBinlog() throws IOException, InterruptedException
    {
        // Compute file name.
        relayLog = new File(relayDir, binlog);
        logger.info("Rotating to new relay log: name="
                + relayLog.getAbsolutePath());

        // Post the name to the log queue. This will block if the extractor
        // is slow and opening another file would cause us to exceeded the relay
        // log retention.
        if (logQueue != null)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Adding relay log file name to log queue: "
                        + relayLog.getAbsolutePath());
            }
            //logger.info("Adding relay log file name to log queue: "
            //        + relayLog.getAbsolutePath());
            logQueue.put(relayLog);
        }

        // Open the file.
        logger.info("Opening relay log: name=" + relayLog.getAbsolutePath());
        try
        {
            this.relayOutput = new FileOutputStream(relayLog);
        }
        catch (FileNotFoundException e)
        {
            logger.error(
                    "Unable to open file for output: "
                            + relayLog.getAbsolutePath(), e);
            return;
        }

        // Write the header.
        relayOutput.write(magic);
        relayOutput.flush();
        relayBytes = 4;

        // Add the file name to the binlog index.
        logger.info("Adding relay log to binlog index: "
                + binlogIndex.getAbsolutePath());

        // Open and read the index file.
        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(binlogIndex, true);
            OutputStreamWriter writer = new OutputStreamWriter(fos);
            @SuppressWarnings("resource")
            PrintWriter printer = new PrintWriter(writer);
            printer.println(relayLog);
            printer.flush();
        }
        finally
        {
            // Close FileOutputStream as this would be the real resource leak.
            if (fos != null)
            {
                try
                {
                    fos.close();
                }
                catch (IOException e)
                {
                }
            }
        }

        // Set the relay log position.
        this.logPosition.setPosition(relayLog, relayBytes);
    }

    // Close the current binlog file.
    private void closeBinlog() throws IOException
    {
        if (relayOutput != null)
        {
            logger.info("Closing relay log: name=" + relayLog.getAbsolutePath()
                    + " bytes=" + relayBytes);
            relayOutput.flush();
            relayOutput.close();
            relayOutput = null;
            relayLog = null;
        }
    }

	public GtidSet getGtidSet() {
		return gtidSet;
	}

	public void setGtidSet(GtidSet gtidSet) {
		this.gtidSet = gtidSet;
	}
	
	private static byte[] hexToByteArray(String uuid) {
        byte[] b = new byte[uuid.length() / 2];
        for (int i = 0, j = 0; j < uuid.length(); j += 2) {
            b[i++] = (byte) Integer.parseInt(uuid.charAt(j) + "" + uuid.charAt(j + 1), 16);
        }
        return b;
    }
}