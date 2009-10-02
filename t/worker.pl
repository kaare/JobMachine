#!/usr/bin/perl

use strict;
use warnings;

use lib 't';
use Jobconfig;
use Worker;

my $config = Jobconfig->new;

my $worker = Worker->new(%$config);
$worker->receive;
