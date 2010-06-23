#!perl

use strict;
use warnings;
use Test::More tests => 3;

my %config = (dsn => 'dbi:Pg:dbname=test', queue => 'qyou',);
ok(my $worker = Worker->new(%config),'New Worker');
isa_ok($worker,'Worker','Worker class');
ok($worker->receive,'receive loop');

package Worker;

use strict;
use warnings;

use base 'Job::Machine::Worker';

sub process {
	my ($self, $data) = @_;
	$self->reply({data => "You've got nail"});
};
