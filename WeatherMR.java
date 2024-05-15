/**
 *
 * @author nivedithaanand
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;


public class WeatherMR {
    public class MapperJoin extends Mapper<Text, Text, Text, Text>{
        Text oKey = new Text();
        Text oValue = new Text();
        Map<String, List<String>> map = new HashMap<>();
        BufferedReader bR;
        
        protected void setUp(Context context) throws IOException{
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for(Path i : files){
                if(i.getName().equals("2006.txt")){
                    bR = new BufferedReader(new FileReader(i.toString()));
                    String input = bR.readLine();
                    while(input!=null){
                        String val[] = input.split(" ");
                        if(!val[0].equals("STN--")){
                            String stn = val[0];
//                            String time = val[1];
//                            String temp = val[2];
//                            String days = val[3];
                            
                            List<String> list = new ArrayList();
                            list.add(val[1]);
                            list.add(val[2]);
                            list.add(val[3]);
                            
                            map.put(stn, list);
                        }
                        input = bR.readLine();
                        
                    }
                }
                bR.close();
            }
        } 
        
        public void map(Text key, Text value, Context context){
            String input[] = value.toString().split(",");
            List<String> val = new ArrayList<String>();
            if(!input[0].contains("Weather") && input[0].contains("usaf")){
            String stn = input[0];
            String cntry = input[3];
            String state = input[4];
            val = map.get(stn);
            oKey.set(stn);
            oValue.set(cntry+" "+state+" "+ val.get(0)+" "+val.get(1)+" "+val.get(2));
            context.write(oKey, oValue);
            }
            
        }
    }

    public static void main(String args[]) {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "weather");
        job.setJarByClass(WeatherMR.class);
        job.setMapperClass(MapperJoin.class);
        DistributedCache.addCacheFile(new URI("hdfs:://localhost:9870/input/2006.txt"), job.getConfiguration());  
        job.setNumReduceTasks(0);
        job.setMapOutputKeyFile(Text.class);
        job.setMapOutputValueFile(Text.class);
        FileInputFormat.addInputPath(job, new Path(pathArgs[0]));   
        FileOutputFormat.setOutputPath(job, new Path(pathArgs[1]));
    }
}
