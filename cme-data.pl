#!/usr/bin/perl

use local::lib '~/perlmods';
use strict;
use DBI;
use LWP::Simple;

chdir File::Basename::dirname(Cwd::abs_path($0));

my $dbFile  = "cme-data.db";
my $dataDir = "data/";
my $cmeFtp  = "ftp://ftp.cmegroup.com/pub/settle/";

my @Files = scalar(@ARGV) ? @ARGV : do{ my $ftp = get $cmeFtp; ($ftp =~ /(?:cme|cbt)\.settle\.\d{8}\.s\.csv\.zip/g)};
my %Dates;

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile","","");
$dbh->sqlite_enable_load_extension(1);
$dbh->sqlite_load_extension("./libwhaley.so","sqlite3_whaley_init");
my $insCSV = $dbh->prepare("INSERT INTO importCSV VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)") or die $dbh->errstr;
my $procDt = $dbh->prepare("INSERT INTO processDate VALUES (?)") or die $dbh->errstr;

foreach my $f (@Files){
	print "$f\n" unless $ENV{QUIET};
	if( scalar(@ARGV) || (!-e $dataDir.$f  && is_success(getstore $cmeFtp.$f, $dataDir.$f)) ){
		$f = $dataDir.$f if !scalar(@ARGV);
		open my $IN, -T $f ? $f : "zcat $f |";
		<$IN>; #throw away header
		$dbh->do("BEGIN");
		while(<$IN>){
			my @data = (/"([^"]*)"/g);
			$Dates{$data[0]} = 1 if $data[0] =~ /^\d{4}-\d{2}-\d{2}$/;
			$insCSV->execute(@data) or die $insCSV->errstr;
		}
		$dbh->do("COMMIT");
		close $IN;
	}
}
foreach my $d (sort keys %Dates){
	print "$d\n" unless $ENV{QUIET};
	$procDt->execute($d) or die $procDt->errstr;
}
$dbh->do("VACUUM");
$dbh->disconnect;
exit;

# CSV file format
# 0     1   2  3      4      5   6     7       8    9    10        11       12           13          14          15        16       17         18        19      20     21         22        23          24        25      26          27       28
# BizDt,Sym,ID,StrkPx,SecTyp,MMY,MatDt,PutCall,Exch,Desc,LastTrdDt,BidPrice,OpeningPrice,SettlePrice,SettleDelta,HighLimit,LowLimit,DHighPrice,DLowPrice,HighBid,LowBid,PrevDayVol,PrevDayOI,FixingPrice,UndlyExch,UndlyID,UndlySecTyp,UndlyMMY,BankBusDay
