#!/usr/bin/perl

use strict;
#use warnings;
use Data::Dumper;

use lib 't';
use Jobconfig;
use Job::Machine::Client;

my $config = Jobconfig->new;
my $client = Job::Machine::Client->new(%$config);
{ no warnings qw/once/;
	*Job::Machine::Client::id = sub {1};
}
if ($client->check('reply')) {
	print Dumper $client->receive;
} else {
	$client->send({data => 'noget snavs'});
};
