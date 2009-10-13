package Worker;

use strict;
use warnings;

use base 'Job::Machine::Worker';

use Data::Dumper;

sub process {
	my ($self, $data) = @_;
	print Dumper $data;
	$self->reply({data => "You've got nail"});
};

1;
