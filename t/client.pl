#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

use lib 't';
use Jobconfig;
use Job::Machine::Client;

my $config = Jobconfig->new;
my $client = Job::Machine::Client->new(%$config);
$client->id(42);
if ($client->check) {
	print Dumper $client->receive;
} else {
	$client->send({data => 'noget snavs'});
};
