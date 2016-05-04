package com.soteradefense.newman;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Created by eickovic on 4/15/16.
 */
public class ParserTest {
    String foo ="{\"foo\":{\"bar\":\"bbq\"}, \"body\": \"\\r\\nenrique garcia mierbanorteclabe 072580006215798578monto 10400 pesos \\t\\t \\t   \\t\\t  \", \"inreplyto\": [], \"bccs_line\": [], \"originating_ips\": [\"[187.192.117.124]\"], \"tos\": [\"martha_chapa@hotmail.com\"], \"tos_line\": [\"<martha_chapa@hotmail.com>\"], \"ccs\": [], \"datetime\": \"2012-02-03T20:27:38\", \"attachments\": [], \"bccs\": [], \"senders\": [\"kikemier@hotmail.com\"], \"ccs_line\": [], \"references\": [], \"messageid\": [\"<SNT128-W416F51E2F563A200BF3FD2D0710@phx.gbl>\"], \"forensic-bcc\": [], \"subject\": \"dlls\", \"id\": \"9dd46c42-0123-11e6-bb05-08002705cb99\", \"categories\": [\"kikemier@hotmail.com\", \"14\", \"5fe6ab1e-6ee0-4fef-98b1-4625d386f702\"], \"senders_line\": [\"enrique garcia <kikemier@hotmail.com>\"]}";

    @Test
    public void parseTest() throws Exception{
        List l = Lists.newArrayList();

        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        Gson gson = new GsonBuilder().create();
        l.add(gson.fromJson(foo, type));
        String r = "";
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try(Writer writer = new OutputStreamWriter(buf, "UTF-8")){
//            Gson gson2 = new GsonBuilder().create();
            gson.toJson(l, writer);
            writer.flush();
            r = buf.toString("utf-8");
        }
        System.out.println(r);

    }
}
