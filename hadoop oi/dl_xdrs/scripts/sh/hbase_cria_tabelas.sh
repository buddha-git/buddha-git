echo "

create 'ns_oilegal:ADSL', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:DIAL', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:FTTX', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:GPRS', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:MMS', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:OCS', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:SMS', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:SYSLOG', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:VOZ_FIXA', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

create 'ns_oilegal:VOZ_MOVEL', {NAME => 'p', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => '15552000'}, MAX_FILESIZE => '76965813944320', SPLITS=> ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

" | hbase shell -n &> hbase_cria_tabelas.log

