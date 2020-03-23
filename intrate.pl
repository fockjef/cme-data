#!/usr/bin/perl

use strict;
use LWP::Simple;
use DBI;

my $dbh = DBI->connect("dbi:SQLite:dbname=cme-data.db","","");
my $insRate = $dbh->prepare("INSERT OR REPLACE INTO intrate(date,r) VALUES (?,?)") or die $dbh->errstr;

$dbh->do("BEGIN");
my $data = get 'https://apps.newyorkfed.org/markets/autorates/fedfundscharttarget';
foreach my $ro ($data =~ /<rateOperation>[\w\W]*?<\/rateOperation>/g){
	my $date = ($ro =~ /<effectiveDate>(\d{4}-\d{2}-\d{2})/)[0];
	my $rate = ($ro =~ /<dailyEffective>([+-\d.]+)/)[0]/100;
	if( $rate <= 0 ){
		$rate = 0.0001;
	}
	$insRate->execute($date,$rate);
}
$dbh->do("END");
$dbh->disconnect;
exit;

