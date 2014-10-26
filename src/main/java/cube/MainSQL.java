/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cube;

import java.sql.Connection ;
import java.sql.DriverManager ;
//import java.sql.ResultSet ;
import java.sql.Statement ;

import lridx.DeNorm ;

import com.hp.hpl.jena.query.ResultSet ;

public class MainSQL {

    public static void main(String[] args) throws Exception {
        
        if ( false ) {
            // User, no password
            // CREATE USER 'afs'@'localhost';
            String s = CubeSQL.createTableString() ;
            System.out.println(s) ;
            System.exit(0) ;
        }
        
        String qs = CubeSQL.createTableString() ;
        
        ResultSet rs = DeNorm.extract() ;
        CubeSQL.build(rs);
    }     
    
    
    public static void misc() throws Exception {        
        String url = "jdbc:mysql://localhost/LR";
        Class.forName ("com.mysql.jdbc.Driver").newInstance ();
        Connection conn = DriverManager.getConnection (url, "afs", "");
        System.out.println("Connected to database");
        Statement stmt = conn.createStatement() ;
        boolean b = stmt.execute("INSERT INTO T VALUES (2, 'bar')") ;
        System.out.println("boolean = "+b) ;
        stmt = conn.createStatement() ;
        java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM T") ;
        while(rs.next()) {
            int val_a = rs.getInt("A") ;
            String val_b = rs.getString("B") ;
            System.out.printf("%d '%s'\n", val_a, val_b) ;
            
        }
        
        System.out.println("DONE");
    }

}

