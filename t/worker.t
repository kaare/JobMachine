#!perl

use strict;
use warnings;

use lib 't';
use Jobconfig;
use Worker;

use Test::More;

plan skip_all => "Currently no worker tests. Figuring out how to make sure there is a server. Also, has to break out of receive loop" unless $ARGV[0];

my $config = Jobconfig->new;
my $worker = Worker->new(%$config);
$worker->receive;
