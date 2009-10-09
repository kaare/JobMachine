package Worker;

use strict;
use warnings;

use base 'Job::Machine::Worker';

use Data::Dumper;

sub process {
	my ($self, $data) = @_;
	print Dumper $data;
	$self->reply({data => 'et svar'});
};

1;
