# Oracle-ArchiveLog-Analyzer
To display modifications to Oracle database contents(DML,DDL) as TEXT FORMAT in the order of Transactions from Online-Redolog or ArchiveLog using LogMiner .  
Oracle database MUST be enabled MIN SUPPLEMENTAL LOGGING   

# INSTALL

```
cpanm DBD::Oracle
git clone https://github.com/kenken0807/Oracle-ArchiveLog-Analyzer.git
```

# USAGE
Options are similer to mysqlbinlog.

```
$ perl Oracle-ArchiveLog-Analyzer.pl
  Usage: perl Oracle-ArchiveLog-Analyzer.pl [options] log-files

  Options:
  --start-position [SCN] Start reading REDO log at SCN. (START_SCN >= #)
  --stop-position  [SCN] Stop reading REDO log at SCN.  (COMMIT_SCN <= #)
  --start-datetime ['YYYY/MM/DD HH24:MI:SS'] Start reading REDO log at first event having a datetime equal. (START_TIMESTAMP  >= 'YYYY/MM/DD HH24:MI:SS')
  --stop-datetime  ['YYYY/MM/DD HH24:MI:SS'] Stop reading REDO log at first event having a datetime equal.  (COMMIT_TIMESTAMP <= 'YYYY/MM/DD HH24:MI:SS')
  --select Show EXPLAIN SELECT STATEMENTS
  --table [TABLENAME] To extract transactions the table has been used (ex."'USERS_TABLE','TEST_TABLE'")
  --xid [transaction id] only show XID
  --in_rollback    include rollback.default is to show only commit transacitions.

  Connect Options:
  --host hostname
  --port port
  --sid  sid
  --user user must be 'system'
  --pass password
  --checkuser FORMAT is "'username','username'...." (ex. --checkuser "'ORCL','ORCLUSER'")

  Detail:
  Set log-files(archivelog or online redolog) at Current Dir or Fullpath
```

```
$ perl Oracle-ArchiveLog-Analyzer.pl  --sid orcl --host 127.0.0.1 --pass xxxx --checkuser "'ORAUSER'" --start-datetime '2016/02/18 14:41:27' --stop-datetime '2016/02/18 15:05:00'  /opt/app/oracle/online-redo/redo02.log

-- START_TIME: 2016/02/18 14:41:27    COMMIT_TIME: 2016/02/18 14:41:42
-- START_SCN: 47902287496      COMMIT_SCN: 47902287503
-- TRANSACTION ID: 09000700172A0600
set transaction read write;
update "ORAUSER"."TT" set "ID2" = '999' where "ID2" = '666';
commit;


-- START_TIME: 2016/02/18 15:04:43    COMMIT_TIME: 2016/02/18 15:04:43
-- START_SCN: 47902288848      COMMIT_SCN: 47902288878
-- TRANSACTION ID: 0A00120003D60B00
create table TEST (id int primary key);


-- START_TIME: 2016/02/18 15:04:53    COMMIT_TIME: 2016/02/18 15:04:53
-- START_SCN: 47902288882      COMMIT_SCN: 47902288883
-- TRANSACTION ID: 0A0006000FD70B00
set transaction read write;
insert into "ORAUSER"."TEST"("ID") values ('999');
commit;


-- START_TIME: 2016/02/18 15:05:00    COMMIT_TIME: 2016/02/18 15:05:00
-- START_SCN: 47902288895      COMMIT_SCN: 47902288924
-- TRANSACTION ID: 0700070050CD0700
truncate table TEST;
```

Use --select to show SELECT statement.
```
$ perl Oracle-ArchiveLog-Analyzer.pl  --sid orcl --host 127.0.0.1 --pass xxxx --checkuser "'ORAUSER'" --start-datetime '2016/02/18 14:41:27' --stop-datetime '2016/02/18 15:04:53'  /opt/app/oracle/online-redo/redo02.log --select


-- START_TIME: 2016/02/18 14:41:27    COMMIT_TIME: 2016/02/18 14:41:42
-- START_SCN: 47902287496      COMMIT_SCN: 47902287503
-- TRANSACTION ID: 09000700172A0600
set transaction read write;
update "ORAUSER"."TT" set "ID2" = '999' where "ID2" = '666';
commit;

-- EXPLAIN SELECT:
-- select count(*) from "ORAUSER"."TT" where "ID2" = '666';


-- START_TIME: 2016/02/18 15:04:43    COMMIT_TIME: 2016/02/18 15:04:43
-- START_SCN: 47902288848      COMMIT_SCN: 47902288878
-- TRANSACTION ID: 0A00120003D60B00
create table TEST (id int primary key);


-- START_TIME: 2016/02/18 15:04:53    COMMIT_TIME: 2016/02/18 15:04:53
-- START_SCN: 47902288882      COMMIT_SCN: 47902288883
-- TRANSACTION ID: 0A0006000FD70B00
set transaction read write;
insert into "ORAUSER"."TEST"("ID") values ('999');
commit;

-- EXPLAIN SELECT:
-- select count(*) from "ORAUSER"."TEST" where "ID" = '999' ;
```
## License
MIT
