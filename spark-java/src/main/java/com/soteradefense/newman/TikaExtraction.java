package com.soteradefense.newman;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tika.exception.EncryptedDocumentException;
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

    public static final Map readJSON(String json){
        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        return Gson.fromJson(json, type);
    }

    public static final String writeJSON(Object attachments) throws IOException{
        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
        try(Writer writer = new OutputStreamWriter(byteBuffer)){
            Gson.toJson(attachments, writer);
            writer.flush();
            return byteBuffer.toString();
        }
    }

    /**
     * Extract text using tika from attachment documents
     * @param docMap
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws TikaException
     */
    public static final Tuple2<String, List> extract(Map<String,Object> docMap) throws IOException, SAXException, TikaException {
        List<Map> attachments = ((List<Map>)docMap.get("attachments"));
        List attachmentsList = Lists.newArrayList();
        for(Map attachment : attachments){
            Object base64Contents = attachment.get("contents64");
            if (base64Contents == null)
                continue;

//          Disable read limit which results in exception:  WriteOutContentHandler.WriteLimitReachedException
            BodyContentHandler handler = new BodyContentHandler(-1);
            byte[] bytes = Base64.decodeBase64(base64Contents.toString());

            try (InputStream stream = new ByteArrayInputStream(bytes)) {
                logger.info(String.format("Parsing doc attachment: doc=%s, attachment=%s", docMap.get("id"), attachment.get("guid").toString()));
                TikaParser.parse(stream, handler, new Metadata(), new ParseContext());

                String extract = handler.toString();
                LanguageIdentifier langIdentifier = new LanguageIdentifier(extract);

                attachmentsList.add(
                        new ImmutableMap.Builder<String, Object>()
                                .put("guid", attachment.get("guid").toString())
                                .put("content", extract)
                                .put("content_tika_langid", langIdentifier.isReasonablyCertain() ? langIdentifier.getLanguage() : "UNKNOWN")
                                .put("content_encrypted", Boolean.FALSE)
                                .put("content_extracted", Boolean.TRUE)
                                .build());
            }catch(EncryptedDocumentException cryptoEx){
                logger.warn(String.format("Parsing encrypted doc: doc=%s, attachment=%s", docMap.get("id"), attachment.get("guid").toString()));
                attachmentsList.add(
                        new ImmutableMap.Builder<String, Object>()
                                .put("guid", attachment.get("guid").toString())
                                .put("content_encrypted", Boolean.TRUE)
                                .put("content_extracted", Boolean.FALSE)
                                .build());
            }catch(Exception e){
                logger.error(String.format("Failed to process attachment for: doc=%s, attachment=%s",docMap.get("id"), attachment.get("guid").toString()), e);
            }
        }
        return new Tuple2(docMap.get("id").toString(), attachmentsList);
    }

    public static void sparkDriver(String inputPath, String outputPath){
        SparkConf sparkConf = new SparkConf().setAppName("Newman attachment text extract");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//        128MB partitions
        ctx.hadoopConfiguration().set("fs.local.block.size", "" + (128 * 1024 * 1024));
        JavaRDD<String> emailJSON = ctx.textFile(inputPath);
        JavaRDD<Map> mapRDD = emailJSON.map(
                s -> readJSON(s)).filter(m -> {
            List<Map> attachments = ((List<Map>)m.get("attachments"));
            if (attachments == null || attachments.isEmpty()){
                logger.info(String.format("Document contains no attachments: doc=%s", m.get("id")));
                return false;
            }
            return true;
        });
        JavaPairRDD<String, List> tuplesRDD = mapRDD.mapToPair(e -> extract(e));
        JavaRDD<String> json = tuplesRDD.map(t -> (t._1() + "\t"+writeJSON(t._2())));

        json.saveAsTextFile(outputPath);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("i", true, "input file path");
        options.addOption("o", true, "output file path");

        CommandLineParser parser = new BasicParser();

        try {
            CommandLine cmd = parser.parse(options, args );
            String inputPath = cmd.getOptionValue("i");
            String outputPath = cmd.getOptionValue("o");
            TikaExtraction tika = new TikaExtraction();
            tika.sparkDriver(inputPath, outputPath);
        }
        catch( org.apache.commons.cli.ParseException exp ) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }


    }
}
