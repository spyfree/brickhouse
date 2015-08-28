package brickhouse.hbase;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import java.util.Map;
import java.util.HashMap;
import java.io.InputStreamReader;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Simple UDF for doing single PUT into HBase table ..
 * Not intended for doing massive reads from HBase,
 * but only when relatively few rows are being read.
 */
@Description(name = "hbase_get",
        value = "_FUNC_(table,key,family) - Do a single HBase Get on a table "
)
public class GetUser extends UDF {


    public String evaluate(String key,String opt) {
        if (opt.equals("type")){
            HashMap<String,String> special_users = new HashMap<String,String>();
            String user_file = "hdfs:///user/hive/data/user_special.tsv";


            try {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(URI.create(user_file),conf);
                FSDataInputStream in = fs.open(new Path(user_file));
                BufferedReader lineReader = new BufferedReader(new InputStreamReader(in));

                String line = null;
                while ((line = lineReader.readLine()) != null){
                    String[] pair = line.split("\t");
                    String type = pair[0];
                    String user_id = pair[1];
                    special_users.put(user_id,type);
                }
            }
            catch (FileNotFoundException e){
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }

            if (special_users.containsKey(key)) {
                return special_users.get(key);
            }
            else
                return "normal";

        }
        else{
            //mobile
            try {
                Map<String, String> config = ImmutableMap.of("table_name","user","family","w","qualifier",opt,"hbase.zookeeper.quorum","sha2dw01,sha2dw02,sha2hdpa02");
                HTable table = HTableFactory.getHTable(config);
                Get theGet = new Get(key.getBytes());
                Result res = table.get(theGet);

                byte[] valBytes = res.getValue(config.get(HTableFactory.FAMILY_TAG).getBytes(), config.get(HTableFactory.QUALIFIER_TAG).getBytes());
                if (valBytes != null) {
                    return new String(valBytes);
                }
                return null;
            } catch (Exception exc) {
                ///LOG.error(" Error while trying HBase PUT ",exc);
                throw new RuntimeException(exc);
            }
        }
    }

}
