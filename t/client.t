#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

use lib 't';
use Jobconfig;
use Job::Machine::Client;

use Test::More;

plan skip_all => "Currently no client tests. Figuring out how to make sure there is a server" unless $ARGV[0];

my $config = Jobconfig->new;
my $client = Job::Machine::Client->new(%$config);
$client->id(42);
if ($client->check) {
	print Dumper $client->receive;
} else {
	$client->send({data => 'noget snavs'});
};
