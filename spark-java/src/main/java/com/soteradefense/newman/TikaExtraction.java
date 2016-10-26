package com.soteradefense.newman;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;
import scala.Tuple2;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

import org.apache.log4j.Logger;

/**
 *
 */
public final class TikaExtraction {
    private final static transient Logger logger = Logger.getLogger(TikaExtraction.class);
    private final static transient AutoDetectParser TikaParser = new AutoDetectParser();
    private final static transient Gson Gson = new GsonBuilder().create();
    private final static HashFunction murmur3Hash = Hashing.murmur3_128();

    public static String hashBytes(byte[] docBytes){
        Hasher hasher = murmur3Hash.newHasher();
        HashCode hash = hasher.putBytes(docBytes).hash();
        return hash.toString();
    }

    public static final Map readJSON(String json){
        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        try{
            Map doc = Gson.fromJson(json, type);
            return doc;
        }catch(JsonSyntaxException jse){
            logger.error(String.format("Failed to read valid email object."), jse);
        }
        return Collections.EMPTY_MAP;
    }

    public static final String writeJSON(Object attachments) throws IOException{
        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
        try(Writer writer = new OutputStreamWriter(byteBuffer)){
            Gson.toJson(attachments, writer);
            writer.flush();
            return byteBuffer.toString();
        }
    }

    public static String sanitizeFieldNamesForES2(String field){
        String sanitized = field.replaceAll(".","_").replaceAll(",","_");
        if (sanitized .startsWith("_") && sanitized .length()>1)
            sanitized = sanitized .substring(1);
        return sanitized;
    }
    //                TODO metadata field contains characters that are illegal in ES fields -- need to go through in detail and extract certain parts based on the doc types
//                TODO this will be somewhat difficult and time consuming
    public static Map copyMetadata(Metadata metadata, boolean extractMetadata){
        if (!extractMetadata || metadata == null)
            return Collections.EMPTY_MAP;
        ImmutableMap.Builder metaBuilder = new ImmutableMap.Builder<String,Object>();
        for (String name : metadata.names()){
            metaBuilder.put(sanitizeFieldNamesForES2(name), metadata.get(name));
        }
        return metaBuilder.build();
    }

    /**
     * Extract text using tika from attachment documents
     * @param docMap
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws TikaException
     */
    public static final Tuple2<String, List> extract(Map<String,Object> docMap, boolean extractMetadata,
                                                     Accumulator<Integer> totalAccum,
                                                     Accumulator<Integer> failureAccum,
                                                     Accumulator<Integer> encryptedAccum)
            throws IOException, SAXException, TikaException {
        List<Map> attachments = ((List<Map>)docMap.get("attachments"));
        List attachmentsList = Lists.newArrayList();
        for(Map attachment : attachments){
            Object base64Contents = attachment.get("contents64");
            if (base64Contents == null)
                continue;

            totalAccum.add(1);

//          Disable read limit which results in exception:  WriteOutContentHandler.WriteLimitReachedException
            BodyContentHandler handler = new BodyContentHandler(-1);
            byte[] bytes = Base64.decodeBase64(base64Contents.toString());

            final String hash = hashBytes(bytes);

//            Only extract if the param is set to true -- Default = false
            Metadata metadata = new Metadata();

            try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
                logger.info(String.format("Parsing doc attachment: doc=%s, attachment=%s, filename=%s", docMap.get("id"), attachment.get("guid").toString(), attachment.containsKey("filename") ? attachment.get("filename").toString() : ""));

                TikaParser.parse(stream, handler, metadata, new ParseContext());

                String extract = handler.toString();
                if(extract.trim().isEmpty())
                    logger.info(String.format("Nothing extracted from attachment: doc=%s, attachment=%s, filename=%s", docMap.get("id"), attachment.get("guid").toString(), attachment.containsKey("filename") ? attachment.get("filename").toString() : ""));

                LanguageIdentifier langIdentifier = new LanguageIdentifier(extract);

                attachmentsList.add(
                        new ImmutableMap.Builder<String, Object>()
                                .put("guid", attachment.get("guid").toString())
                                .put("content", extract)
                                .put("content_length", extract.length())
                                .put("content_tika_langid", langIdentifier.isReasonablyCertain() ? langIdentifier.getLanguage() : "UNKNOWN")
                                .put("content_encrypted", Boolean.FALSE)
                                .put("content_extracted", Boolean.TRUE)
                                .put("content_hash", hash)
                                .put("metadata", TikaExtraction.copyMetadata(metadata, extractMetadata))
                                .put("size", bytes.length)
                                .build());
            }catch(org.apache.tika.exception.EncryptedDocumentException cryptoEx){
                logger.warn(String.format("Parsing encrypted doc: doc=%s, attachment=%s, filename=%s", docMap.get("id"), attachment.get("guid").toString(), attachment.containsKey("filename")?attachment.get("filename").toString(): ""));
                encryptedAccum.add(1);
                attachmentsList.add(
                        new ImmutableMap.Builder<String, Object>()
                                .put("guid", attachment.get("guid").toString())
                                .put("content_length", 0)
                                .put("content_encrypted", Boolean.TRUE)
                                .put("content_extracted", Boolean.FALSE)
                                .put("content_hash", hash)
                                .put("metadata", TikaExtraction.copyMetadata(metadata, extractMetadata))
                                .put("size", bytes.length)
                                .build());
            }catch(TikaException tke){
//              With encrypted pps files tika may throw this with a cause, instead of an EncryptedDocumentException with cause set to the correct exception
                if(tke.getCause() instanceof org.apache.poi.EncryptedDocumentException){

                    encryptedAccum.add(1);
                    logger.warn(String.format("Parsing encrypted doc: doc=%s, attachment=%s, filename=%s", docMap.get("id"), attachment.get("guid").toString(), attachment.containsKey("filename")?attachment.get("filename").toString(): ""), tke);
                    attachmentsList.add(
                            new ImmutableMap.Builder<String, Object>()
                                    .put("guid", attachment.get("guid").toString())
                                    .put("content_length", 0)
                                    .put("content_encrypted", Boolean.TRUE)
                                    .put("content_extracted", Boolean.FALSE)
                                    .put("content_hash", hash)
                                    .put("metadata", TikaExtraction.copyMetadata(metadata, extractMetadata))
                                    .put("size", bytes.length)
                                    .build());
                }else{
                    failureAccum.add(1);
                    logger.error(String.format("Failed to process attachment for: doc=%s, attachment=%s, filename=%s", docMap.get("id"), attachment.get("guid").toString(), attachment.containsKey("filename") ? attachment.get("filename").toString() : ""), tke);
                }
            }catch(Exception e){
                failureAccum.add(1);
                logger.error(String.format("Failed to process attachment for: doc=%s, attachment=%s, filename=%s", docMap.get("id"), attachment.get("guid").toString(), attachment.containsKey("filename") ? attachment.get("filename").toString() : ""), e);
            }catch(NoSuchMethodError nme){
//              This seems to be an error caused by jar mismatch between spark and tika
// TODO         Need to look into this more and add shader plugin for the package
                try{
                    failureAccum.add(1);
                    logger.error(String.format("Failed to process attachment for: doc=%s, attachment=%s, filename=%s, MIME=%s",
                            docMap.get("id"),
                            attachment.get("guid").toString(),
                            attachment.get("filename").toString(),
                            attachment.get("content_type").toString()),
                            nme);
                }catch(Exception e){
                    logger.error(String.format("LOGGING FAILED:  Failure during exception -- doc missing required field! doc=%s",docMap.get("id")));
                }
            }
        }
        return new Tuple2(docMap.get("id").toString(), attachmentsList);
    }

    public static void sparkDriver(String inputPath, String outputPath, boolean extractMetadata){
        SparkConf sparkConf = new SparkConf().setAppName("Newman attachment text extract");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        System.out.println(String.format("SPARK APPLICATION: Name: %s ID: %s", ctx.sc().appName(), ctx.sc().applicationId()));
        logger.info(String.format("SPARK APPLICATION: Name: %s ID: %s", ctx.sc().appName(), ctx.sc().applicationId()));

        Accumulator<Integer> totalAccum = ctx.intAccumulator(0);
        Accumulator<Integer> failureAccum = ctx.intAccumulator(0);
        Accumulator<Integer> encryptedAccum = ctx.intAccumulator(0);

//        128MB partitions
        ctx.hadoopConfiguration().set("fs.local.block.size", "" + (128 * 1024 * 1024));
        JavaRDD<String> emailJSON = ctx.textFile(inputPath);
        JavaRDD<Map> mapRDD = emailJSON.map(
                s -> readJSON(s)).filter(m -> {
            List<Map> attachments = ((List<Map>) m.get("attachments"));
            if (attachments == null || attachments.isEmpty()) {
                logger.info(String.format("Document contains no attachments: doc=%s", m.get("id")));
                return false;
            }
            return true;
        });
        JavaPairRDD<String, List> tuplesRDD = mapRDD.mapToPair(e -> extract(e, extractMetadata, totalAccum, failureAccum, encryptedAccum));
        JavaRDD<String> json = tuplesRDD.map(t -> (t._1() + "\t"+writeJSON(t._2())));

        json.saveAsTextFile(outputPath);

        logger.warn("+++SPARK ACCUMULATOR+++Total attachments: " + totalAccum.value());
        logger.warn("+++SPARK ACCUMULATOR+++Failed attachments: " + failureAccum.value());
        logger.warn("+++SPARK ACCUMULATOR+++Encrypted attachments: " + encryptedAccum.value());

    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("i", true, "input file path");
        options.addOption("o", true, "output file path");
        options.addOption("m", "metadata", false, "add metadata");

        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args );
            String inputPath = cmd.getOptionValue("i");
            String outputPath = cmd.getOptionValue("o");
            boolean extractMetadata = Boolean.parseBoolean(cmd.getOptionValue("m"));

            TikaExtraction tika = new TikaExtraction();
            tika.sparkDriver(inputPath, outputPath, extractMetadata);
        }
        catch( org.apache.commons.cli.ParseException exp ) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }
    }
}
