#!/usr/bin/perl

use strict;
use warnings;
use Test::More tests => 3;

eval "use Test::Pod::Coverage 1.04";
plan skip_all => "Test::Pod::Coverage 1.04 required for testing POD coverage" if $@;
pod_coverage_ok(
	"Job::Machine::$_",
	{ also_private => [ qw/id subscribe/ ], },
	"Job::Machine::$_ is private" ) for (qw/Base Client Worker/);
