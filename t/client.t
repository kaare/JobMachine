#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

eval "use DBD::Pg";
if ($@) {
	plan skip_all => "DBD::Pg required for testing Job::Machine::Client";
} else {
	plan tests => 4;
}

use_ok('Job::Machine::Client','Use Client');

my %config = (dsn => 'dbi:Pg:dbname=test', queue => 'qyou',);
ok(my $client = Job::Machine::Client->new(%config),'New client');
isa_ok($client,'Job::Machine::Client','Client class');
ok(my $id = $client->send({data => 'Try Our Tasty Foobar!'}),'Send a task, no listener');
