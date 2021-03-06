/*
 * Copyright 2011 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.pig;

import com.mongodb.*;
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.output.*;
import com.mongodb.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.*;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;


import java.io.*;
import java.text.ParseException;
import java.util.*;

public class MongoStorage extends StoreFunc implements StoreMetadata {

    private static final Log log = LogFactory.getLog( MongoStorage.class );
    // Pig specific settings
    static final String PIG_OUTPUT_SCHEMA = "mongo.pig.output.schema";
    static final String PIG_OUTPUT_SCHEMA_UDF_CONTEXT = "mongo.pig.output.schema.udf_context";
    protected ResourceSchema schema = null;
    private final MongoStorageOptions options;
    private java.text.DateFormat dt = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'Z");

    public MongoStorage(){ 
        this.options = null;
    }
    
    /**
     * Takes a list of arguments of two types: 
     * <ul>
     * <li>A single set of keys to base updating on in the format:<br />
     * 'update [time, user]' or 'multi [timer, user] for multi updates</li>
     * 
     * <li>Multiple indexes to ensure in the format:<br />
     * '{time: 1, user: 1},{unique: true}'<br />
     * (The syntax is exactly like db.col.ensureIndex())</li>
     * </ul>
     * Example:<br />
     * STORE Result INTO '$db' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:true, dropDups: true}').
     * @param args
     * @throws ParseException
     */
    public MongoStorage(String... args) throws ParseException {
        this.options = MongoStorageOptions.parseArguments(args);
    }


    public void checkSchema( ResourceSchema schema ) throws IOException{
        log.info("checking schema " + schema.toString());
        this.schema = schema;
        final Properties properties =
                UDFContext.getUDFContext().getUDFProperties( this.getClass(), new String[] { _udfContextSignature } );
        properties.setProperty( PIG_OUTPUT_SCHEMA_UDF_CONTEXT, schema.toString());
    }

    public void storeSchema( ResourceSchema schema, String location, Job job ){
        // not implemented
    }


    public void storeStatistics( ResourceStatistics stats, String location, Job job ){
        // not implemented
    }


    public void putNext( Tuple tuple ) throws IOException{
        log.info("writing " + tuple.toString());
        final Configuration config = _recordWriter.getContext().getConfiguration();
        final List<String> schema = Arrays.asList( config.get( PIG_OUTPUT_SCHEMA ).split( "," ) );
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

        ResourceFieldSchema[] fields = this.schema.getFields();
        for (int i = 0; i < fields.length; i++) {
            writeField(builder, fields[i], tuple.get(i));
        }
        
        log.info("writing out:" + builder.get().toString());
        _recordWriter.write( null, builder.get() );
    }

    protected void writeField(BasicDBObjectBuilder builder,
                            ResourceSchema.ResourceFieldSchema field,
                            Object d) throws IOException {

		String fname = field.getName();
		
        // If the field is missing or the value is null, write a null unless 
        if (d == null) {
            if (!this.options.shouldDropNull()) builder.add( field.getName(), d );
            return;
        }

		// field name is the requested id key, rename the fields to internal Mongo id field
		if (fname.equals(this.options.getIdKey()))
		{
			fname = "_id";
		}

		// if field name has the operation prefix, replace prefix with $ sign. 
		if ((this.options.getOpPrefix() != null) && (fname.startsWith(this.options.getOpPrefix())))
		{
			fname = "$"+fname.substring(this.options.getOpPrefix().length());
		}
		
       	ResourceSchema s = field.getSchema();
		java.text.DateFormat dt = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'Z");

        // Based on the field's type, write it out
        switch (field.getType()) {
            case DataType.INTEGER:
                builder.add( fname, (Integer)d );
                return;

            case DataType.LONG:
                builder.add( fname, (Long)d );
                return;

            case DataType.FLOAT:
                builder.add( fname, (Float)d );
                return;

            case DataType.DOUBLE:
                builder.add( fname, (Double)d );
                return;

            case DataType.BYTEARRAY:
                builder.add( fname, d.toString() );
                return;

            case DataType.CHARARRAY:
				String sd = (String)d;
				if(sd.matches("^[0-9-]+T[0-9:.]+Z$")) {
					sd=sd+"+0000";
					try { 
						builder.add( fname, dt.parse(sd)); 
					} catch (Exception e) {}
	            }
				else
				{
	                builder.add( fname, (String)d );
				}

                return;

			case DataType.MAP:
			case DataType.INTERNALMAP:
				Map<Object, Object> mm = (Map<Object, Object>)d;
				Iterator<Map.Entry<Object, Object> > i = mm.entrySet().iterator();

				HashMap h = new HashMap();
				while (i.hasNext()) {
					Map.Entry<Object, Object> entry = i.next();
				    Object v = entry.getValue();
					Object res = null;
					// try to cast as int
				    try { res = Integer.parseInt((String)v); } catch (Exception e) {}
					// try to cast as long
					if (res == null) try { res = Long.parseLong((String)v); } catch (Exception e) {}
					// try as a List
					// try to cast as string (or date if matches date format)
					if (res == null) try { 
						res = (String)v; 
						// check if matches date format, if so convert to date
						if(((String)res).matches("^[0-9-]+T[0-9:.]+Z$")) {
							res=res+"+0000";
							res = dt.parse((String)res); 
			            }						
					} catch (Exception e) {}
					
				    h.put(entry.getKey(),res);
				}
                builder.add( fname, (Map)h );
				break;

            // Given a TUPLE, create a Map so BSONEncoder will eat it
            case DataType.TUPLE:
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use "
                            + "this storage function.  No schema found for field " +
                            fname);
                }
                ResourceSchema.ResourceFieldSchema[] fs = s.getFields();
				builder = builder.push(fname);
		        for (int j = 0; j < fs.length; j++) {
				    Object obj = ((Tuple)d).get(j);
				    writeField(builder,fs[j],obj);
				}
				builder = builder.pop();
                return;

            // Given a BAG, create an Array so BSONEnconder will eat it.
            case DataType.BAG:
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use "
                            + "this storage function.  No schema found for field " +
                            fname);
                }
                fs = s.getFields();
                if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
                    throw new IOException("Found a bag without a tuple "
                            + "inside!");
                }
                // Drill down the next level to the tuple's schema.
                s = fs[0].getSchema();
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use "
                            + "this storage function.  No schema found for field " +
                            fname);
                }
                fs = s.getFields();

                ArrayList a = new ArrayList<Map>();
                for (Tuple t : (DataBag)d) {
				    BasicDBObjectBuilder build = BasicDBObjectBuilder.start();
		            for (int j = 0; j < fs.length; j++) {
						writeField(build,fs[j], ((Tuple) t).get(j));
		            }
		            a.add(build.get());
                }

                builder.add( fname, a);
                return;
        }
    }

    public void prepareToWrite( RecordWriter writer ) throws IOException{

        _recordWriter = (MongoRecordWriter) writer;
        log.info( "Preparing to write to " + _recordWriter );
        if ( _recordWriter == null )
            throw new IOException( "Invalid Record Writer" );
        // Parse the schema from the string stored in the properties object.

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
                udfc.getUDFProperties(this.getClass(), new String[]{_udfContextSignature});

        String strSchema = p.getProperty(PIG_OUTPUT_SCHEMA_UDF_CONTEXT);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        try {
        // Parse the schema from the string stored in the properties object.
            this.schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        if(options != null) {
            // If we are insuring any indexes do so now:
            for (MongoStorageOptions.Index in : options.getIndexes()) {
                _recordWriter.ensureIndex(in.index, in.options);
            }
        }
    }

    public OutputFormat getOutputFormat() throws IOException{
        final MongoOutputFormat outputFmt = options == null ? new MongoOutputFormat() : new MongoOutputFormat(options.getUpdate().keys, options.getUpdate().multi);
        return outputFmt;
    }

    public String relToAbsPathForStoreLocation( String location, org.apache.hadoop.fs.Path curDir ) throws IOException{
        // Don't convert anything - override to keep base from messing with URI
        return location;
    }

    public void setStoreLocation( String location, Job job ) throws IOException{
        final Configuration config = job.getConfiguration();
        log.info( "Store Location Config: " + config + " For URI: " + location );
        if ( !location.startsWith( "mongodb://" ) )
            throw new IllegalArgumentException(
                    "Invalid URI Format.  URIs must begin with a mongodb:// protocol string." );
        MongoConfigUtil.setOutputURI( config, location );
        final Properties properties =
                UDFContext.getUDFContext().getUDFProperties( this.getClass(), new String[] { _udfContextSignature } );
        config.set( PIG_OUTPUT_SCHEMA, properties.getProperty( PIG_OUTPUT_SCHEMA_UDF_CONTEXT ) );
    }

    public void setStoreFuncUDFContextSignature( String signature ){
        _udfContextSignature = signature;
    }

	protected Date parseDate(String dateString) throws Exception
	{
		// FIXME: for now, hardcode ISO8601 with UTC format but introducing a JODA time dependency
		// would give more flexibility
		if(dateString.matches("^[0-9-]+T[0-9:.]+Z$")) {
			dateString+="+0000";
			try { 
			 return dt.parse(dateString); 
			} catch (Exception e) {}
        }

		return null;
	}
    String _udfContextSignature = null;
    MongoRecordWriter _recordWriter = null;
}
