#!/usr/local/bin/perl
use strict;
use utf8;
use DBI;
use Data::Dumper;
use Getopt::Long;

# how to conut DML tables
#
my ($orahost,$oraport,$orasid,$orauser,$orapass,$checkuser,$in_rollback,$in_xid)=("","1521","","system","","",0,"");
my ($startpos,$stoppos,$startdate,$stopdate,$select,$table,$debug);
GetOptions('start-position=s'=> \$startpos,'stop-position=s'=> \$stoppos,'start-datetime=s'=> \$startdate,'stop-datetime=s'=> \$stopdate,'select'=>\$select
         , 'host=s'=> \$orahost,'port=s'=> \$oraport,'sid=s'=> \$orasid,'user=s'=> \$orauser,'password=s'=> \$orapass,'checkuser=s'=> \$checkuser,'table=s'=> \$table,'debug'=>\$debug,'in_rollback'=>\$in_rollback,'xid=s'=> \$in_xid);


#check options
if(!$orahost || ! $oraport || ! $orasid || ! $orauser || ! $orapass )
{
  &Help();
  exit;
}
#checklogfiles
if($#ARGV+1 == 0)
{
  print "[ERROR]Set logfile\n";
  &Help;
  exit;
}

#connect oracle
my $ORADBH=Ora_connect_db($orahost,$oraport,$orasid,$orauser,$orapass);

$table = uc $table if($table);
$checkuser= uc $checkuser if($checkuser);
Main();

sub Help {
  print << "EOS"
  Usage: perl $0 [options] log-files

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

EOS
}

sub Main {
  #GET SCN
  my $SCN;
  #READ REDO
  exit if(&Check_REDO());

  my $commit= $in_rollback ? "" : " + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY" ;
  #Start Analyze REDO
  $ORADBH->do("begin SYS.DBMS_LOGMNR.START_LOGMNR( OPTIONS => SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT $commit) ;  end;");
  print "exec SYS.DBMS_LOGMNR.START_LOGMNR( OPTIONS => SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG  + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT $commit);\n" if($debug);
  #FORMAT SQL
  my $addsql;
  my $chackusers= $checkuser ? " AND  USERNAME in ($checkuser)  " : "";
  $addsql=$addsql." AND START_SCN >= $startpos " if($startpos);
  $addsql=$addsql." AND COMMIT_SCN <= $stoppos " if($stoppos);
  $addsql=$addsql." AND START_TIMESTAMP  >= TO_DATE('$startdate', 'yyyy/mm/dd hh24:mi:ss') " if($startdate);
  $addsql=$addsql." AND COMMIT_TIMESTAMP <= TO_DATE('$stopdate', 'yyyy/mm/dd hh24:mi:ss') " if($stopdate);
  $addsql=$addsql." AND TABLE_NAME in ($table) " if($table);
  $addsql=$addsql." AND xid='$in_xid' " if($in_xid);
  my $getsql=qq{
  SELECT 
    *
  FROM
    (
    SELECT 
      SCN,
      MIN(START_SCN) OVER (PARTITION BY RAWTOHEX(xid)) as START_SCN,
      MAX(COMMIT_SCN) OVER (PARTITION BY RAWTOHEX(xid)) as COMMIT_SCN,
      RAWTOHEX(xid) as xid,
      SEQUENCE#,
      MIN(START_TIMESTAMP) OVER (PARTITION BY RAWTOHEX(xid)) as START_TIMESTAMP,
      MAX(COMMIT_TIMESTAMP) OVER (PARTITION BY RAWTOHEX(xid)) as COMMIT_TIMESTAMP,
      SQL_REDO,
      OPERATION_CODE,
      MIN(OPERATION_CODE) OVER (PARTITION BY RAWTOHEX(xid) ) as OPE_CODE_MIN,
      TABLE_NAME,
      TIMESTAMP,
      MAX(SESSION_INFO) OVER  (PARTITION BY RAWTOHEX(xid) ) as SESSION_INFO
    FROM V\$LOGMNR_CONTENTS 
    WHERE 
      (SEG_OWNER <> 'SYS' or SEG_OWNER is null)
      AND (INFO not like '%INTERNAL%' or INFO is null)
      AND OPERATION_CODE in (1,2,3,5,6,7,36)
      $chackusers
    ) 
  WHERE
    OPE_CODE_MIN <> 6 
  AND (OPE_CODE_MIN <> 5 or OPERATION_CODE = 5) $addsql
  ORDER BY  COMMIT_SCN,xid,SEQUENCE#};

$getsql=qq{
  SELECT
    SCN,START_SCN,COMMIT_SCN,xid,SEQUENCE#,START_TIMESTAMP,COMMIT_TIMESTAMP,SQL_REDO,OPERATION_CODE,OPE_CODE_MIN,TABLE_NAME,TIMESTAMP,SESSION_INFO
  FROM
    (
    SELECT
      SCN,
      MAX(SCN) OVER (PARTITION BY RAWTOHEX(xid)) as MAX_SCN,
      MIN(START_SCN) OVER (PARTITION BY RAWTOHEX(xid)) as START_SCN,
      MAX(COMMIT_SCN) OVER (PARTITION BY RAWTOHEX(xid)) as COMMIT_SCN,
      RAWTOHEX(xid) as xid,
      SEQUENCE#,
      MIN(START_TIMESTAMP) OVER (PARTITION BY RAWTOHEX(xid)) as START_TIMESTAMP,
      MAX(COMMIT_TIMESTAMP) OVER (PARTITION BY RAWTOHEX(xid)) as COMMIT_TIMESTAMP,
      SQL_REDO,
      OPERATION_CODE,
      MIN(OPERATION_CODE) OVER (PARTITION BY RAWTOHEX(xid) ) as OPE_CODE_MIN,
      TABLE_NAME,
      TIMESTAMP,
      MAX(SESSION_INFO) OVER  (PARTITION BY RAWTOHEX(xid) ) as SESSION_INFO
    FROM V\$LOGMNR_CONTENTS
    WHERE
      (SEG_OWNER <> 'SYS' or SEG_OWNER is null)
      AND (INFO not like '%INTERNAL%' or INFO is null)
      AND OPERATION_CODE in (1,2,3,5,6,7,36)
      $chackusers
    )
  WHERE
    OPE_CODE_MIN <> 6
  AND (OPE_CODE_MIN <> 5 or OPERATION_CODE = 5) $addsql
  ORDER BY  MAX_SCN,xid,SEQUENCE#} if($in_rollback);


  print "$getsql\n" if($debug);
  my ($vBaseXid,@vSelectAry);
  my $sth=$ORADBH->prepare($getsql) or die DBI->errstr;
  $sth->execute()  or die DBI->errstr;
  while ( my ($vScn,$vStart,$vCommit,$vXid,$vSeq,$vTime,$vCtime,$vSql,$vOpeCode,$vOpemin,$vTname,$vTimestamp,$vSessInfo)= $sth->fetchrow)
  {
    #transaction changed
    if($vBaseXid eq $vXid)
    {
      print "$vTimestamp :  $vSql\n";
    }
    else
    {
      @vSelectAry=PrintSelect(@vSelectAry) if(@vSelectAry && $select);
      print MakeHeader($vTime,$vCtime,$vStart,$vCommit,$vXid,$vSql,$vTimestamp,$vSessInfo);
      $vBaseXid=$vXid;
    }
    #report select
    my $vSel=CreateSel($vOpeCode,$vSql) if($select);
    push(@vSelectAry,$vSel) if($vSel && $select);
  }
  @vSelectAry=PrintSelect(@vSelectAry) if(@vSelectAry && $select);
}


####MakeHeader
sub MakeHeader {
  my ($vTime,$vCtime,$vStart,$vCommit,$vXid,$vSql,$vTimestamp,$vSessInfo)=(shift,shift,shift,shift,shift,shift,shift,shift);
  my $header=<<"EOS";


-- START_TIME: $vTime    COMMIT_TIME: $vCtime
-- START_SCN: $vStart      COMMIT_SCN: $vCommit
-- TRANSACTION ID: $vXid
-- SESSION INFO: $vSessInfo
$vTimestamp : $vSql 
EOS
  return $header;
}
####Print EXPLAIN SELECT
sub PrintSelect {
  my @vArr=@_;
  my @eArr=();
  print "\n-- EXPLAIN SELECT: \n";
  foreach my $i(@vArr){print "$i\n";}
  return @eArr;
}
####ReplaceVars
sub ReplaceVars {
  my @vars=@_;
  my (@vBufs,$vBuf);
  foreach my $t(@vars)
  {
    if($t=~/^['|"].+['|"]$/ || $t=~/^NULL/)
    {
      if($vBuf)
      {
        push(@vBufs,$vBuf);
        $vBuf="";
      }
      push(@vBufs,$t);
    }
    else
    {
      if($t=~/^\s/ || $t=~/^[0-9]/)
      {
        $vBuf=$vBuf.",".$t;
      }
      else
      {
        push(@vBufs,$vBuf) if($vBuf);
        $vBuf=$t;
      }
    }
  }
  push(@vBufs,$vBuf) if($vBuf);
  return (@vBufs);
}


####CreateSel
sub CreateSel{
  my ($vOpe,$vSql)=(shift,shift);
  my $vNotCreate="-- Can't Create Select Statement";
  my $vNotPrint="-- Can't show because of Too Long";
  my $allWhere;
  my $rtn;
  my $len=3500;
  if($vOpe == 1 ) #INSERT
  {
    if($vSql=~/^insert into (\S+)\(("\S+)\) values \((.+).+;/)
    {
      my @cols=split(/,/, $2);
      my @vars=split(/,/, $3);
      #replacement @vars2 @vars
      @vars=ReplaceVars(@vars) if(scalar(@cols) != scalar(@vars));
      my ($colsLen,$varsLen)=(scalar(@cols),scalar(@vars));
      return $vNotCreate if($colsLen != $varsLen);
      for(my $i=0;$i < $colsLen;$i++)
      {
        my $vWhere = $vars[$i] eq "NULL" ? " $cols[$i] is null " : "$cols[$i] = $vars[$i]";
        $allWhere= $i==0 ? "where $vWhere " : $allWhere." and $vWhere ";
      }
      $rtn= "-- select count(*) from $1 $allWhere;";
      return length($rtn) > $len ? $vNotPrint : $rtn;
    }
  }

  if($vOpe == 2) #DELETE
  {
    if($vSql=~/^delete from (\S+) where (.+);/)
    {
      $rtn= "-- select count(*) from $1 where $2;";
      return length($rtn) > $len ? $vNotPrint : $rtn;
    }
  }

  if($vOpe == 3) #UPDATE
  {
    if($vSql=~/^update (\S+) set.+ where (".+);/)
    {
      $rtn= "-- select count(*) from $1 where $2;";
      return length($rtn) > $len ? $vNotPrint : $rtn;
    }
  }
  return "";
}

####Check_REDO
sub Check_REDO{
  my $pwd=`pwd`;
  chomp($pwd);
  my $new=0;
  foreach my $log (@ARGV)
  {
     $log="$pwd/$log" if($log !~/^\//);
    my $ops=$new ? "SYS.DBMS_LOGMNR.ADDFILE" : "SYS.DBMS_LOGMNR.NEW";
    $ORADBH->do("begin SYS.DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '$log', OPTIONS => $ops);  end;") or die DBI->errstr;
    print "exec SYS.DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '$log', OPTIONS => $ops);\n" if($debug);
    $new=1;
  }
}

####Ora_connect_db
sub Ora_connect_db {
  my $db = join(';',"dbi:Oracle:host=$_[0]","port=$_[1]","sid=$_[2]");
  my $db_uid_passwd = "$_[3]/$_[4]";
  my $dbh = DBI->connect($db, $db_uid_passwd, "") or die DBI->errstr;
  return $dbh;
}
