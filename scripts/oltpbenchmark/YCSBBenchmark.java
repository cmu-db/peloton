/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.ycsb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class YCSBBenchmark extends BenchmarkModule {

    public YCSBBenchmark(WorkloadConfiguration workConf) {
        super("ycsb", workConf, true);
    }

    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        ArrayList<Worker> workers = new ArrayList<Worker>();
        try {
            Connection metaConn = this.makeConnection();

            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF USERS

            //Table t = this.catalog.getTable("USERTABLE");
            //assert (t != null) : "Invalid table name '" + t + "' " + this.catalog.getTables();
            //String userCount = SQLUtil.getMaxColSQL(t, "ycsb_key");
            //Statement stmt = metaConn.createStatement();
            //ResultSet res = stmt.executeQuery(userCount);
            int init_record_count = 3000;
            //while (res.next()) {
            //    init_record_count = res.getInt(1);
            //}
            assert init_record_count > 0;
            //res.close();
            
            for (int i = 0; i < workConf.getTerminals(); ++i) {
//                Connection conn = this.makeConnection();
//                conn.setAutoCommit(false);
                workers.add(new YCSBWorker(i, this, init_record_count + 1));
            } // FOR
            metaConn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return workers;
    }

    @Override
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {
        return new YCSBLoader(this, conn);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        // TODO Auto-generated method stub
        return InsertRecord.class.getPackage();
    }

}
