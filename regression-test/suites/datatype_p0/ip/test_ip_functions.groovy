
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_ip_functions") {
    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """ DROP TABLE IF EXISTS test_ip_functions """

    sql """
    CREATE TABLE test_ip_functions (
      `id` bigint,
      `ip_v4` varchar(20),
      `ip_v6` varchar(40)
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "insert into test_ip_functions values(-1, NULL, NULL)"
    sql "insert into test_ip_functions values(0, '0.0.0.0', '::')"
    sql "insert into test_ip_functions values(2130706433, '127.0.0.1', '2001:1b70:a1:610::b102:2')"
    sql "insert into test_ip_functions values(4294967295, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')"
    sql "insert into test_ip_functions values(4294967296, '255.255.255.256', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffg')"

    qt_sql1 "select id, ip_v4, ipv4stringtonum(ip_v4) from test_ip_functions order by id"
    qt_sql2 "select id, ip_v4, ipv4stringtonum_or_default(ip_v4) from test_ip_functions order by id"
    qt_sql3 "select id, ip_v4, ipv4stringtonum_or_null(ip_v4) from test_ip_functions order by id"
    qt_sql4 "select id, ip_v4, ipv4numtostring(id) from test_ip_functions order by id"
    qt_sql5 "select id, ip_v4, inet_aton(ip_v4) from test_ip_functions order by id"
    qt_sql6 "select id, ip_v4, inet_ntoa(id) from test_ip_functions order by id"

    qt_sql7 "select id, ip_v6, ipv6numtostring(ipv6stringtonum(ip_v6)) from test_ip_functions order by id"
    qt_sql8 "select id, ip_v6, ipv6numtostring(ipv6stringtonum_or_null(ip_v6)) from test_ip_functions order by id"
    qt_sql9 "select id, ip_v6, ipv6numtostring(ipv6stringtonum_or_default(ip_v6)) from test_ip_functions order by id"
    qt_sql10 "select id, ip_v6, inet6_ntoa(inet6_aton(ip_v6)) from test_ip_functions order by id"
    qt_sql11 "select ipv6numtostring(unhex('2A0206B8000000000000000000000011')), ipv6numtostring(unhex('2B020CB8000000001000000100000011'))"
    qt_sql12 "select isipv4string('255.255.255.255'), isipv4string('255.255.255.256'), isipv4string('0.0.0.0.1'), isipv4string('0.0.0.2556'), isipv4string(' '), isipv4string('')"
    qt_sql13 "select isipv6string('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), isipv6string('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffg'), isipv4string(' '), isipv4string('')"
    sql "DROP TABLE test_ip_functions"
}