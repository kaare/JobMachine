package Jobconfig;

use strict;
use warnings;

sub new {
	my ($class) = @_;
	return bless {
		hostname => 'localhost',
		port     => 61613,
		username => 'user',
		password => 'password',
		jobclass => 'job.task',
	}, $class;
}

1;
