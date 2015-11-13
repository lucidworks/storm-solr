package com.lucidworks.storm.solr;

import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class DefaultJsonContentStreamMapper implements JsonContentStreamMapper {

  class PipedInputContentStream extends ContentStreamBase implements Runnable
  {
    private final ObjectMapper mapper;
    private final Object docObj;
    private final PipedOutputStream out;
    private final PipedInputStream in;

    PipedInputContentStream(ObjectMapper mapper, Object docObj) throws IOException {
      this.mapper = mapper;
      this.docObj = docObj;
      this.out = new PipedOutputStream();
      this.in = new PipedInputStream(this.out, 4096);
    }

    public InputStream getStream() throws IOException {
      return in;
    }

    public void run() {
      try {
        mapper.writeValue(out, docObj);
        out.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        try {
          out.close();
        } catch (Exception ignore){
          ignore.printStackTrace();
        }
      }
    }
  }

  public static final String JSON_CONTENT_TYPE = "application/json";

  protected ObjectMapper mapper = new ObjectMapper();

  public ContentStream toContentStream(String docId, Object docObj) throws Exception {
    if (docObj instanceof String) {
      // already a string, so just stream it out directly
      ContentStreamBase.StringStream ss = new ContentStreamBase.StringStream((String)docObj);
      ss.setContentType(JSON_CONTENT_TYPE);
      return ss;
    }

    // pipe the bytes written by the ObjectMapper during JSON serialization to the InputStream
    PipedInputContentStream contentStream = new PipedInputContentStream(mapper, docObj);
    contentStream.setContentType(JSON_CONTENT_TYPE);
    Thread serThread = new Thread(contentStream);
    serThread.start(); // start pumping bytes from the JSON serializer into the input stream using a separate thread
    return contentStream;
  }
}
