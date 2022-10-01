import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.neo4j.driver.*;

import static org.neo4j.driver.Values.parameters;

public class Main {
 //   // class to ingest data in neo4j
    public static class DataIngester implements AutoCloseable
    {
        private final Driver driver;

        public DataIngester( String uri, String user, String password )
        {
            driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
        }

        @Override
        public void close() throws Exception
        {
            driver.close();
        }

        public void writeData( final String id1, final String id2 )
        {
            try ( Session session = driver.session() )
            {
                String greeting = session.writeTransaction( new TransactionWork<String>()
                {
                    @Override
                    public String execute(Transaction tx )
                    {

                        Result result = tx.run(
            "MERGE (a:user{message:$id1}) MERGE (b:user{message:$id2}) MERGE (b)-[:follows]->(a)"  ,
                                parameters( "id1", id1 ,"id2",id2) );

                        return null;
                    }
                } );

            }
        }

    }

    static DataIngester writer = new DataIngester( "bolt://localhost:7687", "neo4j", "12345" );
    private static Path path = new Path("/Users/aakashgoyal/Downloads/twitter.parquet");
    static int i=0;


    private static void printGroup(Group g) {
        int fieldCount = g.getType().getFieldCount();
        String user1=g.getValueToString(0,0);
        String user2=g.getValueToString(1,0);

        writer.writeData(user1 ,user2);
        i++;
        System.out.println(i);

       for (int field = 0; field < fieldCount; field++) {
           var valueCount = g.getFieldRepetitionCount(field);

           Type fieldType = g.getType().getType(field);
           String fieldName = fieldType.getName();

           for (int index = 0; index < valueCount; index++) {
               if (fieldType.isPrimitive()) {
                   System.out.println(fieldName + " " + g.getValueToString(field, index));
               }
           }
       }

    }

    public static void main(String[] args) throws IllegalArgumentException {

        Configuration conf = new Configuration();


        try {
            //reading parquet file
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);
            PageReadStore pages = null;
            try {
                while ( null != (pages = r.readNextRowGroup())) {

                    final long rows = pages.getRowCount();
                    System.out.println("Number of rows: " + rows);
                    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                    for (int i = 0; i < rows ;i++) {  // 
                        final Group g =(Group) recordReader.read();
                        printGroup(g);     //  calling function to create a->b relationship
                    }
                }
            }
            finally {
                r.close();
            }
        } catch (IOException e) {
            System.out.println("Error reading in the parquet file.");
            e.printStackTrace();
        }
    }
}
