package HDP.mapreduce_xml;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger log = LoggerFactory.getLogger(Map.class);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String document = value.toString();
        System.out.println("'" + document + "'");

        try {
            XMLStreamReader reader =
                    XMLInputFactory.newInstance().createXMLStreamReader(new
                            ByteArrayInputStream(document.getBytes()));

            Text propertyName = new Text();
            Text propertyValue = new Text();
            String currentElement = "";

            while (reader.hasNext()) {
                int code = reader.next();
                log.debug("Code: " + code);
                switch (code) {
                case START_ELEMENT:
                    currentElement = reader.getLocalName();
                    break;
                case CHARACTERS:
                    if (currentElement.equalsIgnoreCase("name")) {
                        propertyName.set(reader.getText()); 
                    } else if (currentElement.equalsIgnoreCase("value")) {
                        propertyValue.set(reader.getText());
                    }
                    if ((propertyName.getLength()>0) && (propertyValue.getLength()>0)) {
                        context.write(propertyName, propertyValue);
                        propertyName.clear();
                        propertyValue.clear();
                    }
                    break;
                }
            }
            reader.close();
        } catch (Exception e) {
            log.error("Error processing '" + document + "'", e);
        }
    }
}
