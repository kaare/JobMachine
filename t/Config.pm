package Jobconfig;

use strict;
use warnings;

sub new {
	my ($class) = @_;
	return bless {
		hostname => 'clover.adapt.dk',
		port     => 61613,
		username => 'user',
		password => 'password',
		queue    => '/clientdir/queue/sub',
	}, $class;
}

1;
