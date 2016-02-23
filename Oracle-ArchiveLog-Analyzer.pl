#!/usr/local/bin/perl
use strict;
use utf8;
use DBI;
use Data::Dumper;
use Getopt::Long;

my ($orahost,$oraport,$orasid,$orauser,$orapass,$checkuser)=("localhost","1521","orcl","system","","");
my ($startpos,$stoppos,$startdate,$stopdate,$select,$table);
GetOptions('start-position=s'=> \$startpos,'stop-position=s'=> \$stoppos,'start-datetime=s'=> \$startdate,'stop-datetime=s'=> \$stopdate,'select'=>\$select
         , 'host=s'=> \$orahost,'port=s'=> \$oraport,'sid=s'=> \$orasid,'user=s'=> \$orauser,'password=s'=> \$orapass,'checkuser=s'=> \$checkuser,'table=s'=> \$table);


#check options
if(!$orahost || ! $oraport || ! $orasid || ! $orauser || ! $orapass || ! $checkuser)
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
  --table [TABLENAME] To extract transactions the table has been used (ex."'USER_MASTER','CODE_MASTER'")

  Connect Options:
  --host hostname [default localhost]
  --port port [default 1521]
  --sid  sid [default orcl]
  --user user must be 'system' [default system]
  --pass password 
  --checkuser FORMAT is "'username','username'...." (ex. --checkuser "'ORCLUSER','ORA'")
  
  Detail:
  Set log-files(archivelog or online redolog) at Current Dir or Fullpath

EOS
}

sub Main {
  #READ REDO
  exit if(&Check_REDO());

  #Start Analyze REDO
  $ORADBH->do("begin SYS.DBMS_LOGMNR.START_LOGMNR( OPTIONS => SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT);  end;");
#print "exec SYS.DBMS_LOGMNR.START_LOGMNR( OPTIONS => SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT);\n";

  #FORMAT SQL
  my $addsql;
  $addsql=$addsql." AND START_SCN >= $startpos " if($startpos);
  $addsql=$addsql." AND COMMIT_SCN <= $stoppos " if($stoppos);
  $addsql=$addsql." AND START_TIMESTAMP  >= '$startdate' " if($startdate);
  $addsql=$addsql." AND COMMIT_TIMESTAMP <= '$stopdate' " if($stopdate);
  $addsql=$addsql." AND TABLE_NAME in ($table) " if($table);
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
      TABLE_NAME
    FROM V\$LOGMNR_CONTENTS 
    WHERE 
      USERNAME in ($checkuser)
      AND (SEG_OWNER <> 'SYS' or SEG_OWNER is null)
      AND (INFO not like '%INTERNAL%' or INFO is null)
      AND OPERATION_CODE in (1,2,3,5,6,7,36)
    ) 
  WHERE
    OPE_CODE_MIN <> 6 
  AND (OPE_CODE_MIN <> 5 or OPERATION_CODE = 5) $addsql
  ORDER BY  COMMIT_SCN,xid,SEQUENCE#};

#print "$getsql\n";
  my ($vBaseXid,@vSelectAry);
  my $sth=$ORADBH->prepare($getsql) or die DBI->errstr;
  $sth->execute()  or die DBI->errstr;
  while ( my ($vScn,$vStart,$vCommit,$vXid,$vSeq,$vTime,$vCtime,$vSql,$vOpeCode,$vOpemin)= $sth->fetchrow)
  {
    #transaction changed
    if($vBaseXid eq $vXid)
    {
      print "$vSql\n";
    }
    else
    {
      @vSelectAry=PrintSelect(@vSelectAry) if(@vSelectAry && $select);
      print MakeHeader($vTime,$vCtime,$vStart,$vCommit,$vXid,$vSql);
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
  my ($vTime,$vCtime,$vStart,$vCommit,$vXid,$vSql)=(shift,shift,shift,shift,shift,shift);
  my $header=<<"EOS";


-- START_TIME: $vTime    COMMIT_TIME: $vCtime
-- START_SCN: $vStart      COMMIT_SCN: $vCommit
-- TRANSACTION ID: $vXid
$vSql 
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
#print "begin SYS.DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '$log', OPTIONS => $ops);  end;";
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
