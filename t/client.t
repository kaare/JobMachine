#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 4;

use_ok('Job::Machine::Client','Use Client');

my %config = (dsn => 'dbi:Pg:dbname=test', queue => 'qyou',);
ok(my $client = Job::Machine::Client->new(%config),'New client');
isa_ok($client,'Job::Machine::Client','Client class');
ok($client->send({data => 'Try our tasty Foobar!'}),'Send a task');
# if ($client->check) {
	# print Dumper $client->receive;
# } else {
	# $client->send({data => 'Try our tasty Foobar!'});
# };
# 