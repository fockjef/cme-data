#!/usr/bin/perl

use local::lib '~/perlmods';
use strict;
use File::Basename;
use Cwd 'abs_path';
use DBI;
use LWP::Simple;
use JSON;
use List::MoreUtils qw(uniq);

my $dbFile  = "cme-data.db";
my $dataDir = "data";
my $cmeFtp  = 'ftp://ftp.cmegroup.com/pub/settle/';

# change to proper working directory
chdir scalar((fileparse(abs_path($0)))[1]);

# setup environment
`sqlite3 -init cme-data.sql $dbFile .q` if !-f $dbFile;
`gcc -g -fPIC -shared -O2 libwhaley.c -o libwhaley.so -lm` if !-f "libwhaley.so";
mkdir $dataDir if !-d $dataDir;

# open database, load extensions, and prepare insert statements
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile","","");
$dbh->sqlite_enable_load_extension(1) or die $dbh->errstr;
$dbh->sqlite_load_extension("./libwhaley.so","sqlite3_whaley_init") or die $dbh->errstr;
my $insCSV = $dbh->prepare("INSERT INTO importCSV VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)") or die $dbh->errstr;
my $procDate = $dbh->prepare("INSERT INTO process(function,date) VALUES ('processDate',?)") or die $dbh->errstr;
my $insRate = $dbh->prepare("INSERT OR REPLACE INTO intrate(date,r) VALUES (?,?)") or die $dbh->errstr;

# get interest rate data
my $data = get 'https://websvcgatewayx2.frbny.org/autorates_fedfunds_external/services/v1_0/fedfunds/xml/retrieveLastN?n=5&typ=RATE';
my $series = ($data =~ /(<Series [^>]*FUNDRATE_OBS_POINT="50%"[\w\W]*?<\/Series>)/)[0];
$dbh->do("BEGIN");
foreach my $obs (reverse ($series =~ /<Obs .*?\/>/g)){
	my $date = ($obs =~ /TIME_PERIOD="(\d{4}-\d{2}-\d{2})"/)[0];
	my $rate = ($obs =~ /OBS_VALUE="([+-\d.]+)"/)[0]/100;
	$insRate->execute($date,$rate) or die $insRate->errstr;
	print "$date\t$rate\n"  unless $ENV{QUIET};
}
$dbh->do("COMMIT");

# get list of settlement files
# ftp://ftp.cmegroup.com not working with LWP get (2018-04-10)
# my @Files = scalar(@ARGV) ? @ARGV : do{ my $ftp = get $cmeFtp; ($ftp =~ /(?:cme|cbt)\.settle\.\d{8}\.s\.csv\.zip/g)};
my @Files = scalar(@ARGV) ? @ARGV : do{ my $ftp = `wget -qO - '$cmeFtp'`; uniq ($ftp =~ /(?:cme|cbt)\.settle\.\d{8}\.s\.csv\.zip/g)};

# process settlement files
my %Dates;
my $numIns = 0;
foreach my $f (sort @Files){
	print "$f\n" unless $ENV{QUIET};
	# ftp://ftp.cmegroup.com not working with LWP get (2018-04-10)
	# if( scalar(@ARGV) || (!-e $dataDir.$f  && is_success(getstore $cmeFtp.$f, $dataDir.$f)) ){
	if( scalar(@ARGV) || (!-e "$dataDir/$f"  && !system("wget -qO '$dataDir/$f' '$cmeFtp$f'")) ){
		$f = "$dataDir/$f" if !scalar(@ARGV);
		open my $IN, -T $f ? $f : "zcat $f |";
		<$IN>; #throw away header
		while(<$IN>){
			my @data = (/"([^"]*)"/g);
			$Dates{$data[0]} = 1 if $data[0] =~ /^\d{4}-\d{2}-\d{2}$/;
			$dbh->do("BEGIN") if $numIns++ % 1000000 == 0;
			$insCSV->execute(@data) or die $insCSV->errstr;
			$dbh->do("COMMIT") if $numIns % 1000000 == 0;
		}
		close $IN;
	}
}
$dbh->do("COMMIT") if $numIns % 1000000 != 0;
print "$numIns inserts\n" unless $ENV{QUIET};

# run triggers to process each day's data
foreach my $d (sort keys %Dates){
	print "$d\n" unless $ENV{QUIET};
	$procDate->execute($d) or die $procDate->errstr;
}

# generate cme-data.js
my $json = ($dbh->selectrow_array("SELECT json FROM cmedata"))[0];
open JSON, ">cme-data.js";
print JSON $json;
close JSON;

# cleanup
$dbh->disconnect;
exit;

# CSV file format
# 0     1   2  3      4      5   6     7       8    9    10        11       12           13          14          15        16       17         18        19      20     21         22        23          24        25      26          27       28
# BizDt,Sym,ID,StrkPx,SecTyp,MMY,MatDt,PutCall,Exch,Desc,LastTrdDt,BidPrice,OpeningPrice,SettlePrice,SettleDelta,HighLimit,LowLimit,DHighPrice,DLowPrice,HighBid,LowBid,PrevDayVol,PrevDayOI,FixingPrice,UndlyExch,UndlyID,UndlySecTyp,UndlyMMY,BankBusDay
